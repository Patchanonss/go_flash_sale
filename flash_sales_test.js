import http from 'k6/http';
import { check } from 'k6';

export const options = {
    scenarios: {
        capacity_test: {
            executor: 'ramping-arrival-rate',
            startRate: 1000,
            timeUnit: '1s',
            preAllocatedVUs: 1500,
            maxVUs: 4000,
            stages: [
                { target: 6000, duration: '3s' },  // Step 1: 6k RPS (Warm-up)
                { target: 9000, duration: '3s' },  // Step 2: 9k RPS (Comfort Zone)
                { target: 12000, duration: '4s' }, // Step 3: 12k RPS (Target Record)
                { target: 15000, duration: '4s' }, // Step 4: 15k RPS (Pushing Limits)
                { target: 0, duration: '2s' },     // Cooldown
            ],
        },
    },
    discardResponseBodies: true,
};



const BASE_URL = 'http://127.0.0.1:8080';

// ฟังก์ชันสุ่มตัวเลขแบบเร็วเพื่อสร้างความหลากหลายให้ข้อมูล
function quickRandom() {
    return Math.floor(Math.random() * 1000000);
}

export default function () {
    const randomId = quickRandom();
    const userString = `hacker-${randomId}`;
    const idempotencyKey = `idemp-raw-${randomId}-${Date.now()}`;
    
    // สร้าง IP ปลอมแบบกระจายตัวเต็มที่ทุก Class
    const clientIP = `${Math.floor(Math.random() * 254) + 1}.${Math.floor(Math.random() * 254) + 1}.${Math.floor(Math.random() * 254) + 1}.${Math.floor(Math.random() * 254) + 1}`;

    const url = `${BASE_URL}/api/orders`;
    const payload = JSON.stringify({
        user_id: userString,
        item_id: 1,
        quantity: 1,
        idempotency_key: idempotencyKey,
        // แถมข้อมูลขยะขนาดย่อม (Garbage Data) ไปใน Body เพื่อบังคับให้ Server เปลือง RAM ในการอ่าน JSON
        _meta: "x".repeat(500) 
    });

    const params = {
        headers: {
            'Content-Type': 'application/json',
            'X-Forwarded-For': clientIP,
            'Idempotency-Key': idempotencyKey,
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) HardcoreTester/2.0',
            'Connection': 'close', // ย้ำเตือน HTTP Header ว่ายิงเสร็จแล้วตัดการเชื่อมต่อทันที
        },
    };

    // ยิงแบบไร้ความปราณี (ไม่มี sleep หรือ jitter ขวางกั้น)
    const res = http.post(url, payload, params);

    check(res, {
        'Success (202)': (r) => r.status === 202,
        'Sold Out (409)': (r) => r.status === 409,
        'Server Error (5xx)': (r) => r.status >= 500,
    });
}
