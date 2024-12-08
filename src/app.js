// src/app.js

const express = require('express');
const dotenv = require('dotenv');
const database = require('./database/db');
const webRouter = require('./routes/web.r');
const apiRouter = require('./routes/api.r')

// Tải biến môi trường từ tệp .env
dotenv.config();

// Khởi tạo Express app
const app = express();

// Xác định port từ biến môi trường hoặc sử dụng mặc định 3000
const port = process.env.PORT || 3000;

// Middleware (nếu có)
// Ví dụ: Parse JSON bodies
app.use(express.json());

// Định nghĩa các route (nếu có)
// Ví dụ: Route gốc
app.use(webRouter);
app.use('/api',apiRouter);

// Khởi tạo cơ sở dữ liệu và sau đó khởi động server
database.initDB()
    .then(() => {
        app.listen(port, () => {
            console.log(`Server is listening on port number ${port}`);
        });
    })
    .catch((error) => {
        console.error('Ứng dụng gặp lỗi:', error);
        process.exit(1);
    });
