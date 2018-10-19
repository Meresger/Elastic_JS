/**
 * Created by hadeer on 30/07/15.
 */

function map(CSVlines) {
    output = {}
    for (var i = 0; i < CSVlines.length; i++) {
        var row = CSVlines[i];
        row = row[0].split(',');
        //for(var j=0; j<row.length; j++){
        if (row[0] in output) {
            output[row[0]] = output[row[0]] + parseFloat(row[1])
        } else {
            output[row[0]] = parseFloat(row[1])
        }
    }

    return JSON.stringify(output)
}
function reduce(results) {
     var output = {}
    for (var i = 0; i < results.length; i++) {
        var result=JSON.parse(results[i]);
        var keys = Object.keys(result);
        for(var x =0; x<keys.length; x++){
                        if(keys[x] in output){
                            output[keys[x]]= output[keys[x]]+result[keys[x]];
                        }else{
                            output[keys[x]] = result[keys[x]];
                        }
                    }
                }
     return JSON.stringify(output)
}


