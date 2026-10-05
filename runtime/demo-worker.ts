// NioDB starter worker: REST fetch example
type DemoEvent = {
  id: string;
  name: string;
  data: { message?: string };
};

export default async function (event: DemoEvent) {
  const response = await fetch("__NIODB_BASE_URL__/health", {
    method: "GET",
    signal: AbortSignal.timeout(3000),
  });

  if (!response.ok) {
    throw new Error(`Health API returned ${response.status}`);
  }

  const health = await response.json();
  console.log("Demo REST call completed", {
    eventId: event.id,
    status: response.status,
    health,
  });
}
