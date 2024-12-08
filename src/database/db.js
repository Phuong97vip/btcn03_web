// src/database/db.js

const axios = require('axios');
const pgp = require('pg-promise')({ capSQL: true });
require('dotenv').config(); // Load environment variables

// Debugging: Log environment variables
console.log('Environment Variables Loaded db.js:');
console.log('DB_HOST:', process.env.DB_HOST);
console.log('DB_PORT:', process.env.DB_PORT);
console.log('DB_NAME:', process.env.DB_NAME);
console.log('DB_USER:', process.env.DB_USER);
console.log('DB_PASSWORD:', process.env.DB_PASSWORD);

const config = {
    host: process.env.DB_HOST,          // e.g., 'localhost'
    port: process.env.DB_PORT,          // e.g., 543
    database: process.env.DB_NAME,      // e.g., 'wad2231db'
    user: process.env.DB_USER,          // e.g., 'u21120534'
    password: process.env.DB_PASSWORD,  // e.g., 'r*N97D8J'
    searchPath: ['s21534']               // Ensures all operations are within the 's21534' schema
};

// Initialize pg-promise
const db = pgp(config);

// Function to execute SQL queries
const execute = async (sql, params) => {
    let dbcn = null;
    try {
        dbcn = await db.connect();
        const data = await dbcn.query(sql, params);
        return data;
    } catch (error) {
        throw error;
    } finally {
        if (dbcn) {
            dbcn.done();
        }
    }
};

// Function to create tables within schema s21534
const createTables = async () => {
    try {
        // Define table creation queries
        const tableQueries = [
            // Bảng images
            `
            CREATE TABLE IF NOT EXISTS images (
                id TEXT NOT NULL,
                title TEXT,
                image TEXT,
                PRIMARY KEY (id, image)
            );
            `,
            // Bảng movies
            `
            CREATE TABLE IF NOT EXISTS movies (
                id TEXT PRIMARY KEY,
                title TEXT,
                originaltitle TEXT,
                fulltitle TEXT,
                year TEXT,
                image TEXT,
                releasedate DATE,
                runtimemins INTEGER,
                runtimestr TEXT,
                plot TEXT,
                awards TEXT,
                companies TEXT,
                countries TEXT,
                languages TEXT,
                imdbrating DOUBLE PRECISION,
                imdbratingcount INTEGER,
                boxoffice JSONB,
                plotfull TEXT
            );
            `,
            // Bảng names
            `
            CREATE TABLE IF NOT EXISTS names (
                id TEXT PRIMARY KEY,
                name TEXT,
                role TEXT,
                image TEXT,
                summary TEXT,
                birthdate DATE,
                deathdate DATE,
                awards TEXT,
                height TEXT
            );
            `,
            // Bảng reviews
            `
            CREATE TABLE IF NOT EXISTS reviews (
                movieid TEXT,
                username TEXT,
                warningspoilers BOOLEAN,
                date DATE,
                rate TEXT,
                title TEXT,
                content TEXT,
                FOREIGN KEY (movieid) REFERENCES movies(id) ON DELETE CASCADE
            );
            `,
            // Bảng directors
            `
            CREATE TABLE IF NOT EXISTS directors (
                movieid TEXT,
                namesid TEXT,
                FOREIGN KEY (movieid) REFERENCES movies(id) ON DELETE CASCADE,
                FOREIGN KEY (namesid) REFERENCES names(id) ON DELETE CASCADE
            );
            `,
            // Bảng writers
            `
            CREATE TABLE IF NOT EXISTS writers (
                movieid TEXT,
                namesid TEXT,
                FOREIGN KEY (movieid) REFERENCES movies(id) ON DELETE CASCADE,
                FOREIGN KEY (namesid) REFERENCES names(id) ON DELETE CASCADE
            );
            `,
            // Bảng actors
            `
            CREATE TABLE IF NOT EXISTS actors (
                movieid TEXT,
                namesid TEXT,
                ascharacter TEXT,
                FOREIGN KEY (movieid) REFERENCES movies(id) ON DELETE CASCADE,
                FOREIGN KEY (namesid) REFERENCES names(id) ON DELETE CASCADE
            );
            `,
            // Bảng genreList
            `
            CREATE TABLE IF NOT EXISTS genrelist (
                movieid TEXT,
                type TEXT,
                FOREIGN KEY (movieid) REFERENCES movies(id) ON DELETE CASCADE
            );
            `,
            // Bảng fav
            `
            CREATE TABLE IF NOT EXISTS fav (
                movieid TEXT PRIMARY KEY,
                FOREIGN KEY (movieid) REFERENCES movies(id) ON DELETE CASCADE
            );
            `,
            // Bảng castMovies
            `
            CREATE TABLE IF NOT EXISTS castmovies (
                namesid TEXT,
                movieid TEXT,
                role TEXT,
                FOREIGN KEY (namesid) REFERENCES names(id) ON DELETE CASCADE,
                FOREIGN KEY (movieid) REFERENCES movies(id) ON DELETE CASCADE
            );
            `
        ];

        // Execute each table creation query
        for (const query of tableQueries) {
            await execute(query);
        }

        console.log('Các bảng đã được tạo hoặc đã tồn tại trong schema "s21534".');

        // Đảm bảo rằng tất cả các cột cần thiết đều tồn tại trong bảng movies
        const alterTablesQueries = [
            // Bảng movies: Thêm cột nếu chưa tồn tại
            `
            ALTER TABLE movies
            ADD COLUMN IF NOT EXISTS originaltitle TEXT;
            `,
            `
            ALTER TABLE movies
            ADD COLUMN IF NOT EXISTS fulltitle TEXT;
            `,
            `
            ALTER TABLE movies
            ADD COLUMN IF NOT EXISTS releasedate DATE;
            `,
            `
            ALTER TABLE movies
            ADD COLUMN IF NOT EXISTS runtimemins INTEGER;
            `,
            `
            ALTER TABLE movies
            ADD COLUMN IF NOT EXISTS runtimestr TEXT;
            `,
            `
            ALTER TABLE movies
            ADD COLUMN IF NOT EXISTS imdbratingcount INTEGER;
            `,
            `
            ALTER TABLE movies
            ADD COLUMN IF NOT EXISTS plotfull TEXT;
            `
            // Bạn có thể thêm các lệnh ALTER TABLE tương tự cho các bảng khác nếu cần
        ];

        // Thực hiện các lệnh ALTER TABLE
        for (const query of alterTablesQueries) {
            await execute(query);
        }

        console.log('Các cột bổ sung đã được thêm vào bảng "movies" nếu chúng chưa tồn tại.');
    } catch (error) {
        console.error('Lỗi khi tạo bảng hoặc thêm cột:', error);
        throw error;
    }
};

// Function to fetch data from APIs
const fetchDataFromAPI = async () => {
    try {
        const [moviesRes, namesRes, reviewsRes, top50Res, mostPopularRes] = await Promise.all([
            axios.get(process.env.MOVIES_API_URL || 'http://matuan.online:2422/api/Movies'),
            axios.get(process.env.NAMES_API_URL || 'http://matuan.online:2422/api/Names'),
            axios.get(process.env.REVIEWS_API_URL || 'http://matuan.online:2422/api/Reviews'),
            axios.get(process.env.TOP50_API_URL || 'http://matuan.online:2422/api/Top50Movies'),
            axios.get(process.env.MOST_POPULAR_API_URL || 'http://matuan.online:2422/api/MostPopularMovies')
        ]);

        return {
            Movies: moviesRes.data,
            Names: namesRes.data,
            Reviews: reviewsRes.data,
            Top50Movies: top50Res.data,
            MostPopularMovies: mostPopularRes.data
        };
    } catch (error) {
        console.error('Error fetching data from API:', error);
        throw error;
    }
};

// Helper function to insert data only if array is non-empty
const insertIfNotEmpty = async (t, data, columnSet, tableName) => {
    if (data && data.length > 0) {
        const query = pgp.helpers.insert(data, columnSet) + ' ON CONFLICT DO NOTHING';
        await t.none(query);
        console.log(`Đã chèn ${data.length} bản ghi vào bảng "${tableName}".`);
    } else {
        console.log(`Không có dữ liệu để chèn vào bảng "${tableName}".`);
    }
};

// Function to insert data into tables
const insertData = async (data) => {
    try {
        await db.tx(async t => {
            // **1. Insert Names and Images**
            if (data.Names && data.Names.length > 0) {
                const names = data.Names.map(name => ({
                    id: name.id,
                    name: name.name || null,
                    role: name.role || null,
                    image: name.image || null,
                    summary: name.summary || null,
                    birthdate: name.birthDate ? new Date(name.birthDate) : null,
                    deathdate: name.deathDate ? new Date(name.deathDate) : null,
                    awards: name.awards || null,
                    height: name.height || null
                }));

                const namesCS = new pgp.helpers.ColumnSet(['id', 'name', 'role', 'image', 'summary', 'birthdate', 'deathdate', 'awards', 'height'], { table: 'names' });
                await insertIfNotEmpty(t, names, namesCS, 'names');

                // Insert Images for Names (Nếu có)
                const allNameImages = data.Names.flatMap(name => {
                    if (name.images && Array.isArray(name.images)) {
                        return name.images.map(image => ({
                            id: name.id,
                            title: image.title || null,
                            image: image.image || null
                        }));
                    }
                    return [];
                });

                if (allNameImages.length > 0) {
                    const imagesCS = new pgp.helpers.ColumnSet(['id', 'title', 'image'], { table: 'images' });
                    await insertIfNotEmpty(t, allNameImages, imagesCS, 'images');
                }

                // **Không chèn CastMovies ở đây**
                // Chúng ta sẽ chèn CastMovies sau khi tất cả các movie đã được chèn
            }

            // **2. Insert Movies from all sources (/api/Movies, /api/Top50Movies, /api/MostPopularMovies)**
            const allMovies = [
                ...(data.Movies || []),
                ...(data.Top50Movies || []),
                ...(data.MostPopularMovies || [])
            ];

            if (allMovies.length > 0) {
                const movies = allMovies.map(movie => ({
                    id: movie.id,
                    title: movie.title,
                    originaltitle: movie.originalTitle || null,
                    fulltitle: movie.fullTitle || null,
                    year: movie.year,
                    image: movie.image || null,
                    releasedate: movie.releaseDate ? new Date(movie.releaseDate) : null,
                    runtimemins: movie.runtimeMins ? parseInt(movie.runtimeMins) : null,
                    runtimestr: movie.runtimeStr || null,
                    plot: movie.plot || null,
                    awards: movie.awards || null,
                    companies: movie.companies || null,
                    countries: movie.countries || null,
                    languages: movie.languages || null,
                    imdbrating: movie.ratings && movie.ratings.imDb ? parseFloat(movie.ratings.imDb) : null,
                    imdbratingcount: movie.ratings && movie.ratings.imDbRatingCount ? parseInt(movie.ratings.imDbRatingCount) : null,
                    boxoffice: movie.boxOffice || null,
                    plotfull: movie.plotFull || null
                }));

                const moviesCS = new pgp.helpers.ColumnSet(['id', 'title', 'originaltitle', 'fulltitle', 'year', 'image', 'releasedate', 'runtimemins', 'runtimestr', 'plot', 'awards', 'companies', 'countries', 'languages', 'imdbrating', 'imdbratingcount', 'boxoffice', 'plotfull'], { table: 'movies' });
                await insertIfNotEmpty(t, movies, moviesCS, 'movies');

                // Insert Images for Movies (Nếu có)
                const allMovieImages = allMovies.flatMap(movie => {
                    if (movie.images && Array.isArray(movie.images)) {
                        return movie.images.map(image => ({
                            id: movie.id,
                            title: image.title || null,
                            image: image.image || null
                        }));
                    }
                    return [];
                });

                if (allMovieImages.length > 0) {
                    const imagesCS = new pgp.helpers.ColumnSet(['id', 'title', 'image'], { table: 'images' });
                    await insertIfNotEmpty(t, allMovieImages, imagesCS, 'images');
                }
            }

            // **3. Insert Reviews**
            if (data.Reviews && data.Reviews.length > 0) {
                const reviews = data.Reviews.flatMap(review => {
                    if (review.items && Array.isArray(review.items)) {
                        return review.items.map(item => ({
                            movieid: review.movieId,
                            username: item.username || null,
                            warningspoilers: item.warningSpoilers || false,
                            date: item.date ? new Date(item.date) : null,
                            rate: item.rate || null,
                            title: item.title || null,
                            content: item.content || null
                        }));
                    }
                    return [];
                });

                const reviewsCS = new pgp.helpers.ColumnSet(['movieid', 'username', 'warningspoilers', 'date', 'rate', 'title', 'content'], { table: 'reviews' });
                await insertIfNotEmpty(t, reviews, reviewsCS, 'reviews');
            }

            // **4. Insert Directors**
            if (allMovies.length > 0) {
                const directors = allMovies.flatMap(movie => {
                    if (movie.directorList && Array.isArray(movie.directorList)) {
                        return movie.directorList.map(director => ({
                            movieid: movie.id,
                            namesid: director.id
                        }));
                    }
                    return [];
                });

                // Lọc các directors có namesid tồn tại
                const validDirectors = [];
                for (const director of directors) {
                    const exists = await t.oneOrNone('SELECT id FROM names WHERE id = $1', [director.namesid]);
                    if (exists) {
                        validDirectors.push(director);
                    } else {
                        console.warn(`namesid ${director.namesid} không tồn tại trong bảng 'names'. Bỏ qua chèn vào 'directors'.`);
                    }
                }

                const directorsCS = new pgp.helpers.ColumnSet(['movieid', 'namesid'], { table: 'directors' });
                await insertIfNotEmpty(t, validDirectors, directorsCS, 'directors');
            }

            // **5. Insert Writers**
            if (allMovies.length > 0) {
                const writers = allMovies.flatMap(movie => {
                    if (movie.writerList && Array.isArray(movie.writerList)) {
                        return movie.writerList.map(writer => ({
                            movieid: movie.id,
                            namesid: writer.id
                        }));
                    }
                    return [];
                });

                // Lọc các writers có namesid tồn tại
                const validWriters = [];
                for (const writer of writers) {
                    const exists = await t.oneOrNone('SELECT id FROM names WHERE id = $1', [writer.namesid]);
                    if (exists) {
                        validWriters.push(writer);
                    } else {
                        console.warn(`namesid ${writer.namesid} không tồn tại trong bảng 'names'. Bỏ qua chèn vào 'writers'.`);
                    }
                }

                const writersCS = new pgp.helpers.ColumnSet(['movieid', 'namesid'], { table: 'writers' });
                await insertIfNotEmpty(t, validWriters, writersCS, 'writers');
            }

            // **6. Insert Actors**
            if (allMovies.length > 0) {
                const actors = allMovies.flatMap(movie => {
                    if (movie.actorList && Array.isArray(movie.actorList)) {
                        return movie.actorList.map(actor => ({
                            movieid: movie.id,
                            namesid: actor.id,
                            ascharacter: actor.asCharacter || null
                        }));
                    }
                    return [];
                });

                // Lọc các actors có namesid và movieid tồn tại
                const namesRows = await t.any('SELECT id FROM names');
                const namesSet = new Set(namesRows.map(row => row.id));

                const moviesRows = await t.any('SELECT id FROM movies');
                const moviesSet = new Set(moviesRows.map(row => row.id));

                // Lọc các actors có namesid và movieid tồn tại
                const validActors = actors.filter(actor => {
                    let valid = true;
                    if (!namesSet.has(actor.namesid)) {
                        console.warn(`namesid ${actor.namesid} không tồn tại trong bảng 'names'. Bỏ qua chèn vào 'actors'.`);
                        valid = false;
                    }
                    if (!moviesSet.has(actor.movieid)) {
                        console.warn(`movieid ${actor.movieid} không tồn tại trong bảng 'movies'. Bỏ qua chèn vào 'actors'.`);
                        valid = false;
                    }
                    return valid;
                });

                const actorsCS = new pgp.helpers.ColumnSet(['movieid', 'namesid', 'ascharacter'], { table: 'actors' });
                await insertIfNotEmpty(t, validActors, actorsCS, 'actors');
            }

            // **7. Insert GenreList**
            if (allMovies.length > 0) {
                const genres = allMovies.flatMap(movie => {
                    if (movie.genreList && Array.isArray(movie.genreList)) {
                        return movie.genreList.map(genre => ({
                            movieid: movie.id,
                            type: genre.value
                        }));
                    }
                    return [];
                });

                const genresCS = new pgp.helpers.ColumnSet(['movieid', 'type'], { table: 'genrelist' });
                await insertIfNotEmpty(t, genres, genresCS, 'genrelist');
            }

            // **8. Insert CastMovies**
            if (data.Names && data.Names.length > 0) {
                const allCastMovies = data.Names.flatMap(name => {
                    if (name.castMovies && Array.isArray(name.castMovies)) {
                        return name.castMovies.map(movie => ({
                            namesid: name.id,
                            movieid: movie.id,
                            role: movie.role || null
                        }));
                    }
                    return [];
                });

                // Lấy tất cả namesid và movieid đã tồn tại
                const namesRows = await t.any('SELECT id FROM names');
                const namesSet = new Set(namesRows.map(row => row.id));

                const moviesRows = await t.any('SELECT id FROM movies');
                const moviesSet = new Set(moviesRows.map(row => row.id));

                // Lọc các castmovies có namesid và movieid tồn tại
                const validCastMovies = allCastMovies.filter(cast => {
                    let valid = true;
                    if (!namesSet.has(cast.namesid)) {
                        console.warn(`namesid ${cast.namesid} không tồn tại trong bảng 'names'. Bỏ qua chèn vào 'castmovies'.`);
                        valid = false;
                    }
                    if (!moviesSet.has(cast.movieid)) {
                        console.warn(`movieid ${cast.movieid} không tồn tại trong bảng 'movies'. Bỏ qua chèn vào 'castmovies'.`);
                        valid = false;
                    }
                    return valid;
                });

                const castMoviesCS = new pgp.helpers.ColumnSet(['namesid', 'movieid', 'role'], { table: 'castmovies' });
                await insertIfNotEmpty(t, validCastMovies, castMoviesCS, 'castmovies');
            }

        
        });
        console.log('Dữ liệu đã được chèn vào cơ sở dữ liệu.');
    } catch (error) {
        console.error('Lỗi khi chèn dữ liệu:', error);
        throw error;
    }
};

// Function to initialize the database
const initDB = async () => {
    try {
        // Create tables in schema s21534
        await createTables();

        // Check if data already exists
        const dataExists = await db.oneOrNone('SELECT 1 FROM movies LIMIT 1');

        if (dataExists) {
            console.log('Dữ liệu đã tồn tại trong cơ sở dữ liệu. Bỏ qua bước chèn dữ liệu.');
            return; // Exit the function early
        }

        // Fetch data from APIs
        const apiData = await fetchDataFromAPI();

        // Insert data into tables
        await insertData(apiData);

        console.log('Database đã được khởi tạo và dữ liệu đã được chèn thành công.');
    } catch (error) {
        console.error('Lỗi khi khởi tạo database:', error);
        throw error;
    }
};

// Export functions for use in other modules
module.exports = {
    execute,
    initDB
};
