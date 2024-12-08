const Review = require('../models/review.m')

module.exports = {
    GetReview: async (req,res,next) => {
        try {
            const data = await Review.getReview(req.params.per_page,req.params.page,req.params.id);
            res.json(data)
        } catch (error) {
            next(error);
        };
        
    }
}