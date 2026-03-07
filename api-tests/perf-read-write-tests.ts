import http from 'k6/http';
import { sleep, check } from 'k6';

export const options = {
    vus: 10,
    duration: '30s',
};

export default function () {
    const testUrl = __ENV.TEST_URL ?? 'http://localhost:8080/v1/series/upsert';
    const tagName = `tag-perf-test-${__VU}`;
    const res = http.post(testUrl, JSON.stringify([
        {
            "tag": tagName,
            "ts": 1772535582000, //UTC_EPOCH_MS
            "pld": {
                "nV": Math.random() * 100
            }
        }
    ]));
    check(res, { "status is 201": (res) => res.status === 201 });
    sleep(1);
}
