const db = require('../database/db')

module.exports = class Cast {
    constructor(obj) {
        this.id = obj.id;
        this.name = obj.name;
        this.role = obj.role;
        this.image = obj.image;
        this.summary = obj.summary;
        this.birthdate = obj.birthdate;
        this.deathdate = obj.deathdate;
        this.awards = obj.awards;
        this.height = obj.height;
    }

    static async detail(id) {
        try {
            const result = await db.execute(`select * from names where id = '${id}';`)
            return result;
        } catch (error) {
            throw(error);
        };
    }

    static async getMovies(id) {
        try {
            const result = await db.execute(`select * from castMovies a join movies b on a.movieid = b.id where namesid = '${id}'`)
            return result;
        } catch (error) {
            throw(error);
        };
    }
}