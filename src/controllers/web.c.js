const Movies = require('../models/movies.m')
const Cast = require('../models/cast.m')

let darkMode = false;

module.exports = {
    index: async (req, res, next) => {
        try {
            const topRating = await Movies.getTopRating(5);
            if(req.params.dark != undefined) darkMode = (req.params.dark == 'true'? true: false)
            res.render('index', { buttons: topRating.carControl, topRatingList: topRating.movies,darkMode: darkMode })
        } catch (error) {
            next(error);
        };

    },
    detailMovies: async (req, res, next) => {
        try {
            if(req.params.dark != undefined) darkMode = (req.params.dark == 'true'? true: false)
            const detail = await Movies.detail(req.params.id)
            let hasData = false;
            if (detail.movies.length != 0) hasData = true;
            res.render('filmDetail', { hasData: hasData, movies: detail.movies[0],darkMode: darkMode })
        } catch (error) {
            next(error);
        };
    },
    detailActor: async (req, res, next) => {
        try {
            const detail = await Cast.detail(req.params.id)
            let hasData = false;
            if (detail.length != 0) hasData = true;
            if(req.params.dark != undefined) darkMode = (req.params.dark == 'true'? true: false)
            res.render('actorDetail', { hasData: hasData, actor: detail[0],darkMode: darkMode })
        } catch (error) {
            next(error);
        };
    },
    postSearch: async (req, res, next) => {
        try {
            if(req.params.dark != undefined) darkMode = (req.params.dark == 'true'? true: false)
            res.redirect(`/search/str=${(req.body.searchString == ''?  ' ' : req.body.searchString)}/type=${req.body.searchType}/dark=${darkMode}`);
        } catch (error) {
            next(error);
        };
    },
    getSearch: async (req, res, next) => {
        try {
            if(req.params.dark != undefined) darkMode = (req.params.dark == 'true'? true: false)
            res.render('search',{type: req.params.type,str: req.params.str,darkMode: darkMode});
        } catch (error) {
            next(error);
        };
    },
    fav: async (req, res, next) => {
        try {
            if(req.params.dark != undefined) darkMode = (req.params.dark == 'true'? true: false)
            res.render('fav',{darkMode: darkMode});
        } catch (error) {
            next(error);
        };
    },
}