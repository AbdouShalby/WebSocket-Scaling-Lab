// k6 WebSocket soak test — complements bench/connect-storm.js.
// connect-storm measures E2E broadcast latency precisely; this k6 scenario
// stresses connection churn (connect / subscribe / hold / disconnect cycles),
// which is what actually kills WS servers in production.
//
// Run:  k6 run load-tests/k6-ws-test.js
//       k6 run -e WS_URL=ws://localhost:8080 -e VUS=2000 load-tests/k6-ws-test.js

import ws from 'k6/ws';
import { check } from 'k6';
import { Counter, Trend } from 'k6/metrics';

const URL = __ENV.WS_URL || 'ws://localhost:8080';
const VUS = parseInt(__ENV.VUS || '500');

const eventsReceived = new Counter('events_received');
const subscribeLatency = new Trend('subscribe_ack_ms', true);

export const options = {
  scenarios: {
    churn: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: '30s', target: VUS },  // ramp up
        { duration: '60s', target: VUS },  // hold
        { duration: '15s', target: 0 },    // drain
      ],
    },
  },
  thresholds: {
    subscribe_ack_ms: ['p(95)<250'],
    ws_connecting: ['p(95)<500'],
  },
};

export default function () {
  const channel = `product:${(__VU % 20) + 1}`;

  const res = ws.connect(URL, { compression: '' }, (socket) => {
    let subscribedAt = null;

    socket.on('open', () => {
      subscribedAt = Date.now();
      socket.send(JSON.stringify({ action: 'subscribe', channel }));
    });

    socket.on('message', (raw) => {
      const msg = JSON.parse(raw);
      if (msg.type === 'subscribed') {
        subscribeLatency.add(Date.now() - subscribedAt);
      } else if (msg.type === 'event') {
        eventsReceived.add(1);
      }
    });

    // Hold the connection 20-40s then leave (connection churn)
    socket.setTimeout(() => socket.close(), 20000 + Math.random() * 20000);
  });

  check(res, { 'ws session established (101)': (r) => r && r.status === 101 });
}
