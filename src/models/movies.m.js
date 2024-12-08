const db = require('../database/db')

module.exports = class Movies {
    constructor(obj) {
        this.id = obj.id;
        this.title = obj.title;
        this.originalTitle = obj.originaltitle;
        this.fullTitle = obj.fulltitle;
        this.year = obj.year;
        this.image = obj.image;
        this.releaseDate = obj.releasedate;
        this.runtimeStr = obj.runtimestr;
        this.plot = obj.plot;
        this.awards = obj.awards;
        this.companies = obj.companies;
        this.countries = obj.countries;
        this.languages = obj.languages;
        this.imDbRating = obj.imdbrating;
        this.boxOffice = obj.boxoffice;
        this.plotFull = obj.plot;
    }
    static async getAll() {
        const result = await db.execute(`select * from movies`)
        const data = {
            total: parseInt(result.length),
            total_page: Math.ceil(result.length / 2),
            movies: result.map((m) => {
                return new Movies(m);
            }),
        };
        return data;
    }
    static async detail(id) {
        const result = await db.execute(`SELECT * FROM movies WHERE id = '${id}';`);
        return {
            movies: result.map((m) => {
                return new Movies(m);
            }),
        }
    }
    static async getSearchMV(perPage, page, searchString, type) {
        const offSet = (page - 1) * perPage;
        searchString = searchString.trim();
        let total;
        let result
        if (type == 'Actor') {
            total = await db.execute(`select count(*) from movies m join actors a on m.id = a.movieid
                                                            join names n on n.id = a.namesid
                                                            where n.name ilike '%${searchString}%'`);
            result = await db.execute(`select m.* from movies m join actors a on m.id = a.movieid
                                                            join names n on n.id = a.namesid
                                                            where n.name ilike '%${searchString}%' LIMIT ${perPage} OFFSET ${offSet};`)
        } else if (type == 'Title') {
            total = await db.execute(`SELECT count(*) FROM movies WHERE title ILIKE '%${searchString}%';`);
            result = await db.execute(`SELECT * FROM movies WHERE title ILIKE '%${searchString}%' LIMIT ${perPage} OFFSET ${offSet};`)

        } else if (type == 'Type') {
            total = await db.execute(`SELECT count(*) FROM movies m join genrelist g on m.id = g.movieid where g.type ilike '%${searchString}%';`);
            result = await db.execute(`SELECT m.* FROM movies m join genrelist g on m.id = g.movieid WHERE type ilike '%${searchString}%' LIMIT ${perPage} OFFSET ${offSet};`);
        }
        return {
            perPage: perPage,
            page: page,
            nPage: Math.ceil(total[0].count / perPage),
            movies: result.map((m) => {
                return new Movies(m);
            }),
        }
    }
    static async getTopRating(perPage) {
        const result = await db.execute(`SELECT * FROM movies ORDER BY imDbRating DESC LIMIT ${perPage} ;`);
        return {
            perPage: perPage,
            movies: result.map((m) => {
                return new Movies(m);
            }),
        }
    }
    static async getTopFav(perPage) {
        const result = await db.execute(`SELECT distinct  m.* FROM movies m join fav f on m.id = f.movieid ORDER BY imDbRating DESC LIMIT ${perPage} ;`);
        return {
            perPage: perPage,
            movies: result.map((m) => {
                return new Movies(m);
            }),
        }
    }
    static async getTopBoxOffice(perPage) {
        let result = await db.execute(`SELECT * FROM movies;`);
        result = result.sort((a, b) => {
            const grossObj1 = Number(a.boxoffice.replace(/[^0-9.-]+/g, ""));
            const grossObj2 = Number(b.boxoffice.replace(/[^0-9.-]+/g, ""));
            return grossObj2 - grossObj1;
        })
        result = result.slice(0, perPage);
        return {
            perPage: perPage,
            movies: result.map((m) => {
                return new Movies(m);
            }),
        }
    }
    static async getPic(id) {
        let result = await db.execute(`SELECT distinct * FROM images where id = '${id}';`);
        return result;
    }
    static async getDirector(id) {
        let result = await db.execute(`SELECT distinct name,id FROM directors d JOIN names n ON d.namesid = n.id where movieid = '${id}';
        `);
        return result;
    }
    static async getWriter(id) {
        let result = await db.execute(`SELECT distinct name,id FROM writers w JOIN names n ON w.namesid = n.id where movieid = '${id}';
        `);
        return result;
    }
    static async getActor(id) {
        let result = await db.execute(`select  distinct name, id from actors a join names n on a.namesid = n.id where movieid = '${id}';
        `);
        return result;
    }
    static async getFav(perPage, page) {
        const offSet = (page - 1) * perPage;
        let total;
        let result
        total = await db.execute(`SELECT count(*) FROM movies m join  fav f on m.id = f.movieid;`);
        result = await db.execute(`SELECT distinct m.* FROM movies m join  fav f on m.id = f.movieid LIMIT ${perPage} OFFSET ${offSet};`)
        return {
            perPage: perPage,
            page: page,
            nPage: Math.ceil(total[0].count / perPage),
            movies: result.map((m) => {
                return new Movies(m);
            }),
        }
    }
    static async addFav(id) {
        try {
            await db.execute(`INSERT INTO fav(
            movieid)
            VALUES ('${id}');`);
            return true;
        } catch (error) {
            return false;
        };
    }


}