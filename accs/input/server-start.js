var express = require('express');
var bodyParser = require('body-parser');

var port = process.env.PORT || 8888;

var app = express();

app.use(bodyParser.json());

app.get('/atl', function (request, response) {
 
   response.send("You landed on the moon");
});

app.get('/atl/:helloMsg', function (request, response) {
    var msg = request.params.helloMsg
    if (msg != null ) {
        response.send("Received message " + msg);
    } else {
        response.status(404);
        response.send("not found");
    }
});


var server = app.listen(port);

