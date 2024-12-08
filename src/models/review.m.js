const db = require('../database/db')

module.exports = class Review {
    constructor(obj) {
        this.username = obj.username;
        this.warningspoilers = obj.warningspoilers;
        this.date = obj.date;
        this.rate = obj.rate;
        this.title = obj.title;
        this.content = obj.content;
    }
    static async getReview(perPage, page, id) {
        const offSet = (page - 1) * perPage;
        const total = await db.execute(`SELECT count(*) FROM reviews WHERE movieid = '${id}';`);
        const result = await db.execute(`SELECT * FROM reviews WHERE movieid = '${id}' LIMIT ${perPage} OFFSET ${offSet};`)
        return {
            perPage: perPage,
            page: page,
            total: parseInt(total[0].count),
            reviews: result.map((r) => {
                return new Review(r);
            }),
        }
    }

}