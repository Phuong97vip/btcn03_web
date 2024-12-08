const Cast = require('../models/cast.m')

module.exports = {
    GetMovies: async (req,res,next) => {
        try {
            const data = await Cast.getMovies(req.params.id);
            res.json(data)
        } catch (error) {
            next(error);
        };
    }
}