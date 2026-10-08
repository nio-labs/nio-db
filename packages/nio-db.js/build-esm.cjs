const fs = require("node:fs");
const path = require("node:path");
const source = fs.readFileSync(path.join(__dirname, "index.js"), "utf8");
const marker = "module.exports = {\n  NioDB,\n  createNioDB,\n};";
if (!source.includes(marker)) throw new Error("CommonJS export marker missing");
fs.writeFileSync(path.join(__dirname, "index.mjs"), source.replace(marker, "export { NioDB, createNioDB };"));
