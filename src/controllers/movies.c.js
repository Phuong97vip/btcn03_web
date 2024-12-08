const Movies = require('../models/movies.m')

module.exports = {
    TopRatingMV: async (req, res, next) => {
        try {
            const topRating = await Movies.getTopRating(req.params.n);
            res.json(topRating)
        } catch (error) {
            next(error);
        };
    },
    TopFav: async (req, res, next) => {
        try {
            const topRating = await Movies.getTopFav(req.params.n);
            res.json(topRating)
        } catch (error) {
            next(error);
        };
    },
    TopBoxOffice: async (req, res, next) => {
        try {
            const topboxoffice = await Movies.getTopBoxOffice(req.params.n);
            res.json(topboxoffice)
        } catch (error) {
            next(error);
        };
    },
    Detail: async (req, res, next) => {
        try {
            const detail = await Movies.detail(req.params.id);
            res.json(detail)
        } catch (error) {
            next(error);
        };
    },
    GetPic:  async (req, res, next) => {
        try {
            const detail = await Movies.getPic(req.params.id);
            res.json(detail)
        } catch (error) {
            next(error);
        };
    },
    GetDirector:  async (req, res, next) => {
        try {
            const detail = await Movies.getDirector(req.params.id);
            res.json(detail)
        } catch (error) {
            next(error);
        };
    },
    GetWriter:  async (req, res, next) => {
        try {
            const detail = await Movies.getWriter(req.params.id);
            res.json(detail)
        } catch (error) {
            next(error);
        };
    },
    GetActor: async (req, res, next) => {
        try {
            const detail = await Movies.getActor(req.params.id);
            res.json(detail)
        } catch (error) {
            next(error);
        };
    },
    search: async (req, res, next) => {
        try {
            const detail = await Movies.getSearchMV(req.params.per_page,req.params.page,req.params.str,req.params.type);
            res.json(detail)
        } catch (error) {
            next(error);
        };
    },
    fav: async (req, res, next) => {
        try {
            const detail = await Movies.getFav(req.params.per_page,req.params.page);
            res.json(detail)
        } catch (error) {
            next(error);
        };
    },
    AddFav: async (req, res, next) => {
        try {
            const detail = await Movies.addFav(req.params.id);
            res.json(detail)
        } catch (error) {
            next(error);
        };
    },

}