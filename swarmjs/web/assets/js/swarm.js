/**
 * Created by sherif on 06/06/15.
 */

var wsocket;
var currentRole;
var connection = false;
var csvData;
var final_map_result;
var new_msg = {
    type: "",
    role: "",
    job_id: ""
};
function createScriptTag(data) {

    var scriptTag = document.createElement("script");
    scriptTag.setAttribute("data-script-url", "data.js");
    scriptTag.setAttribute("type", "text/javascript");
    var txt = document.createTextNode(data);
    scriptTag.appendChild(txt);
    return scriptTag;
}

function addDynamicScript(data) {
    document.head.appendChild(createScriptTag(data));
}

function connect() {
    if (connection) {
        return;
    }
    console.log(" " + window.location.host);
    var host = "ws://" + window.location.host + "/ws"
    try {

        function parseNestMsg(msg) {
            m = JSON.parse(msg.data);

            if (m.type == 'connection_ready') {
                $("label[for='worker_log']").text("Worker ID: "+ m.swarm_id);
                new_msg.type = "client_role";
                new_msg.role = currentRole;
                new_msg.job_id = $('#jobref').val();
                console.log("send role")
                wsocket.send(JSON.stringify(new_msg));
            }
            else if (m.type == 'Data') {
                csvData = m.data;
                $("#joblog").val($("#joblog").val() + "\n" + "Received data from ELASTIC JS Job Server");
                console.log("receved data")

            }
            else if (m.type == 'Code') {
                var code = (m.data)
                addDynamicScript(code)
                $("#joblog").val($("#joblog").val() + "\n" + "Received Script from ELASTIC JS Job Server");
                $("#joblog").val($("#joblog").val() + "\n" + "Injecting Script");


                map_result = map(csvData);
                new_msg.type = "work_result"
                new_msg.data = map_result
                console.log("the map result is " + map_result)
                $("#joblog").val($("#joblog").val() + "\n" + "Sending result to ELASTIC JS Job Server");

                $("#joblog").val($("#joblog").val() + "\n" + map_result);
                wsocket.send(JSON.stringify(new_msg))
                console.log("received code")

            }
            else if (m.type == 'job_result') {
                $("#joblog").val($("#joblog").val() + "\n" + "Received mappers results");
                $("#joblog").val($("#joblog").val() + "\n" + m.result);

                final_map_result = m.result
                console.log("got array")
            }
            else if (m.type == 'rCode') {
                var code = (m.data)
                addDynamicScript(code)
                uploader_result = reduce(final_map_result);
                //bootbox.alert("Final Result:\n"+uploader_result,null);
                showFinal(uploader_result);
                console.log("Your result is " + uploader_result);


            }
        }

        wsocket = new WebSocket(host)

        wsocket.onopen = function () {
            console.log("connection is open");
            connection = true;
        }

        wsocket.onmessage = function (msg) {

            parseNestMsg(msg);


        }
        wsocket.onclose = function () {
            console.log("connection is closed");
            connection = false;

        }

    } catch (exception) {
        connection = false;
    }
}

$(document).ready(function () {

    if (!("WebSocket" in window)) {
        console.log("Sorry thi web browser does not support web socket");
    }
});

$('#start_job').click(function () {
    currentRole = "job_owner";
    //console.log($('#jobref').val());
    connect();
});

$('#start_worker').click(function () {
    currentRole = "job_worker";
    connect();
});

function showFinal(final) {
    var msg= final.split(",");
    var out="";
    for (var x =0; x<msg.length; x++){
        out+=msg[x]+"\n";
    }
    bootbox.dialog({
        animate: true,
        message: out,
        className: "medium",
        title: "Job Complete",
        buttons: {
            success: {
                label: "Success!",
                className: "btn-success",
                callback: function () {

                }
            }
        }

    });
}