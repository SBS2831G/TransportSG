function performQuery() {
    let query = $('#input').value;
    if (query.trim() ==  '') return;

    let url = location.pathname;
    $('#loading').style.display = 'block';

    $.ajax({
        url,
        method: 'POST',
        data: {
            query
        }
    }, (content) => {
        $('#loading').style.display = 'none';

        $('#results').innerHTML = content;
    });
}

$.ready(() => {
    let inputTimeout = 0;

    let input = $('#input');

    input.on('input', () => {
        clearTimeout(inputTimeout);
        inputTimeout = setTimeout(performQuery, 850);
    });

    performQuery();
});
