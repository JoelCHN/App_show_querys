$('#carouselExampleIndicators').on('slide.bs.carousel', function (e) {
    var nextTitle = $(e.relatedTarget).find('.carousel-caption').text();
    $('.carousel-caption').text(nextTitle);
});