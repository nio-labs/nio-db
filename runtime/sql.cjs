'use strict';
// Compile a deliberately small SQL grammar into fixed syntax and bound values.
// Raw user SQL is never handed to AlaSQL's JavaScript compiler.
const forbidden = new Set(['__proto__', 'constructor', 'prototype']);
function identifier(value) {
  if (!/^[A-Za-z_][A-Za-z0-9_]{0,63}$/.test(value) || forbidden.has(value.toLowerCase())) throw new Error('query_rejected');
  return value;
}
const quote = (value) => `[${identifier(value)}]`;

function tokens(sql) {
  if (typeof sql !== 'string' || sql.length > 65536) throw new Error('query_rejected');
  const result = [];
  let i = 0;
  while (i < sql.length) {
    if (/\s/.test(sql[i])) { i++; continue; }
    if (sql[i] === '-' && sql[i + 1] === '-') {
      while (i < sql.length && sql[i] !== '\n') i++;
      continue;
    }
    if (sql[i] === "'") {
      let value = ''; i++;
      let closed = false;
      while (i < sql.length) {
        if (sql[i] === "'") {
          if (sql[i + 1] === "'") { value += "'"; i += 2; continue; }
          i++; closed = true; break;
        }
        value += sql[i++];
      }
      if (!closed) throw new Error('query_rejected');
      result.push({ kind: 'value', value }); continue;
    }
    const rest = sql.slice(i);
    const word = /^[A-Za-z_][A-Za-z0-9_]*/.exec(rest);
    if (word) { result.push({ kind: 'id', value: identifier(word[0]) }); i += word[0].length; continue; }
    const number = /^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?/.exec(rest);
    if (number) {
      const value = Number(number[0]);
      if (!Number.isFinite(value) || Math.abs(value) > Number.MAX_SAFE_INTEGER) throw new Error('query_rejected');
      result.push({ kind: 'value', value }); i += number[0].length; continue;
    }
    const symbol = /^(?:<=|>=|<>|!=|[=<>?*.,();])/.exec(rest);
    if (!symbol) throw new Error('query_rejected');
    result.push({ kind: 'symbol', value: symbol[0] }); i += symbol[0].length;
    if (result.length > 1024) throw new Error('query_rejected');
  }
  if (result.length > 1024) throw new Error('query_rejected');
  return result;
}

function compileSql(sql, parameters, tables) {
  const stream = tokens(sql);
  if (!Array.isArray(parameters) || parameters.length > 100 || !tables || typeof tables !== 'object') throw new Error('query_rejected');
  const bindings = [];
  let at = 0, parameter = 0, depth = 0, joins = 0;
  const peek = (value) => stream[at] && stream[at].kind !== 'value' && stream[at].value.toLowerCase() === value;
  const take = (value) => { if (!peek(value)) return false; at++; return true; };
  const expect = (value) => { if (!take(value)) throw new Error('query_rejected'); };
  const name = () => { const token = stream[at++]; if (!token || token.kind !== 'id') throw new Error('query_rejected'); return identifier(token.value); };
  function column(allowStar = false) {
    if (allowStar && take('*')) return '*';
    let result = quote(name());
    if (take('.')) result += '.' + (allowStar && take('*') ? '*' : quote(name()));
    return result;
  }
  function expression(aggregate) {
    if (stream[at]?.kind === 'id' && stream[at + 1]?.value === '(') {
      const fn = name().toUpperCase();
      if (!(aggregate ? ['COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'LOWER', 'UPPER', 'LEN'] : ['LOWER', 'UPPER', 'LEN']).includes(fn)) throw new Error('query_rejected');
      expect('('); const argument = column(fn === 'COUNT'); expect(')');
      return `${fn}(${argument})`;
    }
    return column(aggregate);
  }
  function scalar() {
    let value;
    if (take('?')) {
      if (parameter >= parameters.length) throw new Error('query_rejected');
      value = parameters[parameter++];
    } else if (stream[at]?.kind === 'value') value = stream[at++].value;
    else if (take('null')) value = null;
    else if (take('true')) value = true;
    else if (take('false')) value = false;
    else return expression(false);
    if (value !== null && !['string', 'boolean', 'number'].includes(typeof value)) throw new Error('query_rejected');
    bindings.push(value); return '?';
  }
  function predicate() {
    if (++depth > 20) throw new Error('query_rejected');
    let result;
    if (take('(')) { result = '(' + disjunction() + ')'; expect(')'); }
    else {
      const left = expression(false);
      if (take('is')) { const not = take('not'); expect('null'); result = `${left} IS ${not ? 'NOT ' : ''}NULL`; }
      else {
        const op = stream[at++];
        if (!op || !['=', '!=', '<>', '<', '>', '<=', '>=', 'like'].includes(op.value.toLowerCase()) || op.kind === 'value') throw new Error('query_rejected');
        result = `${left} ${op.value.toUpperCase()} ${scalar()}`;
      }
    }
    depth--; return result;
  }
  function conjunction() { let result = predicate(); while (take('and')) result += ' AND ' + predicate(); return result; }
  function disjunction() { let result = conjunction(); while (take('or')) result += ' OR ' + conjunction(); return result; }
  function table() {
    let key = name();
    if (take('.')) key = name();
    const source = key === 'records' ? 'artifacts' : key;
    if (!Object.hasOwn(tables, source) || !Array.isArray(tables[source])) throw new Error('query_rejected');
    bindings.push(tables[source]);
    let alias = key;
    if (take('as')) alias = name();
    else if (stream[at]?.kind === 'id' && !['left', 'inner', 'join', 'where', 'group', 'order', 'limit', 'offset'].includes(stream[at].value.toLowerCase())) alias = name();
    return `? AS ${quote(alias)}`;
  }
  expect('select');
  const distinct = take('distinct');
  const columns = [];
  do {
    let field = expression(true);
    if (take('as')) field += ' AS ' + quote(name());
    columns.push(field);
    if (columns.length > 32) throw new Error('query_rejected');
  } while (take(','));
  expect('from');
  let from = table();
  while (peek('left') || peek('inner') || peek('join')) {
    if (++joins > 2) throw new Error('query_rejected');
    let join = 'INNER';
    if (take('left')) { join = 'LEFT'; take('outer'); } else take('inner');
    expect('join'); from += ` ${join} JOIN ${table()}`; expect('on'); from += ' ON ' + conjunction();
  }
  const where = take('where') ? ' WHERE ' + disjunction() : '';
  let group = '';
  if (take('group')) { expect('by'); const fields = []; do { fields.push(column()); } while (take(',')); group = ' GROUP BY ' + fields.join(', '); }
  let order = '';
  if (take('order')) {
    expect('by'); const fields = [];
    do { let field = column(); if (take('desc')) field += ' DESC'; else { take('asc'); field += ' ASC'; } fields.push(field); } while (take(','));
    order = ' ORDER BY ' + fields.join(', ');
  }
  function integer(maximum) {
    const token = stream[at++];
    if (!token || token.kind !== 'value' || !Number.isInteger(token.value) || token.value < 0 || token.value > maximum) throw new Error('query_rejected');
    return token.value;
  }
  const limit = take('limit') ? integer(1000) : 1000;
  if (limit < 1) throw new Error('query_rejected');
  const offset = take('offset') ? integer(1000000) : 0;
  take(';');
  if (at !== stream.length || parameter !== parameters.length) throw new Error('query_rejected');
  return { sql: `SELECT ${distinct ? 'DISTINCT ' : ''}${columns.join(', ')} FROM ${from}${where}${group}${order} LIMIT ${limit} OFFSET ${offset}`,
    bindings, limit, countSql: group ? null : `SELECT COUNT(*) AS [total] FROM ${from}${where}` };
}

module.exports = { compileSql, identifier };
