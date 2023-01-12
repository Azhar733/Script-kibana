const express=require('express');
const elasticsearch = require('elasticsearch');

var mysql = require('mysql');

var con = mysql.createConnection({
  host: " localhost",
  user: "root",
  password: "root",
  database: "elastic"
});

con.connect(function(err) {
//   if (err) throw err;
//   con.query("SELECT * FROM disposecall", function (err, result, fields) {
//     if (err) throw err;
//     console.log(result);

con.query("SELECT cdrid FROM disposecall", function (err, result, fields) {
    
        if (err) throw err;
        var a = result
        console.log(a) 

 
      
    })
    
  });
// });



// const client = require('./server/elasticSearch/client.js');

// const {conn}=require('./connection.js')
// // const { Client } = require('elasticsearch')


// const db=require('./database/db.js')

//  const data=require('./routes/routes.js');
// // const client = require('./server/elasticSearch/client.js');

const app=express();
const  ELASTIC_SEARCH_URL='https://slashadmin:FlawedByDesign@1612$@elastic-50-uat.slashrtc.in/elastic' ;
console.log( ELASTIC_SEARCH_URL)




app.listen(8000,console.log("server connected successfully"))


const connect = async () => {
 client = new elasticsearch.Client({
    host: ELASTIC_SEARCH_URL,
    log: { type: 'stdio', levels: [] }
  });
  return client;
};



const ping = async () => {
  let attempts = 0;
  const pinger = ({ resolve, reject }) => {
    attempts += 1;
    client
      .ping({ requestTimeout: 30000 })
      .then(() => {
        console.log('Elasticsearch server available');
        resolve(true);
      })
      .catch(() => {
        if (attempts > 100) reject(new Error('Elasticsearch failed to ping'));
        console.log('Waiting for elasticsearch server...');
        setTimeout(() => {
          pinger({ resolve, reject });
        }, 1000);
      });
  };

  return new Promise((resolve, reject) => {
    pinger({ resolve, reject });
  })
}


const connection=async()=>{
    try{
      await connect();
      await ping();
    }catch(error){
      console.log(error)
    }
  }
  
//   let uid="26fe698a-c50d-4543-ae27-a20c45a46921"
  connection()
  
  async function run() {
    const response = await client.search({
      index: 'azharsk',
      body: {
        query: {
          bool: {
            must: [
              {
                match: {
                  "callinfo.agentLegUuid.keyword": `${a}`,
                }
              }
            ]
          }
        }
      }
    })
  
    console.log(response.hits.hits)
  }

  async function updateElastic(z){
  const update = {
    script: {
      source: 
             `ctx._source.callinfo.callTime.talkTime = ${z.agent_talktime_sec}`,

    },
    query: {
      bool: {
        must: {
          match: {
            "callinfo.agentLegUuid.keyword":`${z.cdrid}`,
          },
        },
      },
    },
  };
  client
    .updateByQuery({
      index: "azharsk",
      body: update,
    })
    .then(
      (res) => {
        console.log("Success", res);
      },
      (err) => {
        console.log("Error", err);
      }
    );
  
    }

    async function streamSQLData() {
        const stream = con.query("SELECT * FROM disposecall").stream();
        stream.on("data", async (m) => {
          await updateElastic(m);
        });
        stream.on("end", () => {
            // all rows have been received
            console.log("All rows have been received");
          });
        }
        
        streamSQLData();
    
  
  //'26fe698a-c50d-4543-ae27-a20c45a46919'
  run().catch(console.log);