const restify = require('restify');
const server = restify.createServer();
const PORT = 8888;
const elasticsearch = require('elasticsearch');
//const url="localhost:9200";
const url="localhost:9200"

const client= new elasticsearch.Client({
    host:url,
});

const mock_data= [
	{
		id : '123',
		name : 'hello',
		menu : [{name:'chicken burger',category:'burger'},{name:'paratha roll',category:'desi'}],
		city : 'lahore'


	},
	{
		id : '12345',
		name : 'world',
		menu : [{name:'testing',category:'wierd shit'}],
		city : 'lahore'


	},
	{
		id : '13423',
		name : 'what world',
		menu : [{name:'chowmein ',category:'chineese'},{name:'tikka',category:'bbq'}],
		city : 'lahore'


	},
	{
		id : '122133',
		name : 'are you ',
		menu : [{name:'beef burger',category:'beef burger'},{name:'sada paratha',category:'desi'}],
		city : 'Islamabad'


	},
	{
		id : '1221313',
		name : 'doing',
		menu : [{name:'fish',category:'seafood'},{name:'prawns',category:'fish'}],
		city : 'Islamabad'


	},
	{
		id : '12312',
		name : 'now',
		menu : [{name:'sushi',category:'wierd shit'},{name:'ping1',category:'urdu'}],
		city : 'Karachi'


	},

]
const axios = require('axios');
const async = require('async');
const index_name='haseeb9';
server.use(restify.plugins.queryParser());
fetchAllDocuments();

function worker(){
    setInterval(fetchAllDocuments,86400000);
}
worker();
client.ping({
    requestTimeout:1000
},function(error){
    if(error){
        console.trace('elastic search cluster is down');
    }else {
        console.log('elastic search is working');
        //client.indices.delete({index:index_name}).then(deleted=>{console.log('deleted')})
    }
});
client.indices.exists({
    index:index_name,
}).then(function(something){
    if(!something){
        client.indices.create({
            index:index_name,
            body:{
                settings:{
                    number_of_shards:15,
                    analysis:{
                        filter:{
                            autocomplete_filter:{
                                type:"edge_ngram",
                                min_gram:1,
                                max_gram:20
                            },
                        },
                        analyzer:{
                            autocomplete:{
                                type:"custom",
                                tokenizer:"standard",
                                filter:[
                                    "lowercase",
                                    "autocomplete_filter"
                                ]
                            },
                            my_lowercase_analyzer:{
                            	type:"custom",
                            	tokenizer:"standard",
                            	filter:["lowercase"]
                            }
                        }
                    }
                },
                mappings:{
                    my_type:{
                        properties:{
                            name:{
                                type:'text',
                                analyzer:"autocomplete",
                                search_analyzer:"standard"
                            },
                            menu:{
                                type:"nested",
                                properties:{
                                    category:{
                                        type:"text",
                                        analyzer:"autocomplete",
                                        search_analyzer:"standard"
                                    },
                                    name:{
                                        type:"text",
                                        analyzer:"autocomplete",
                                        search_analyzer:"standard",
                                    }
                                }
                            },
                            city:{
                            	type:'text',
                            	analyzer:'my_lowercase_analyzer'
                            }
                        }
                    }
                }
            }
        }).then(function(result){
            fetchAllDocuments();
        }).catch(function(error){
            console.log('error:',error);
        });
    }else{
        console.log('index already exists',index_name);
    }
}).catch(function(error){
    console.log(error);
});
function fetchAllDocuments(){
    axios.get('https://api.paitoo.com.pk/restaurants/all').then(function(response){
    	response.data = mock_data;
        async.each(response.data,function(restaurant,callback){

        	/*
            axios.get('https://api.paitoo.com.pk/restaurants/restaurant/'+restaurant._id
            ).then(function(response){
			*/
                if(restaurant._id){
                    restaurant.id=restaurant._id;
                    delete restaurant._id;
                }
                //console.log(restaurant);
                client.exists({
                    index:index_name,
                    type:'my_type',
                    id:restaurant.id
                },function(error,exists){
                	if(error)
                		console.log('error in exists',error,'asdf',restaurant)
                	// console.log(exists);
                    if(exists === false){
                    	console.log(exists)
                        client.create({
                            index:index_name,
                            type:'my_type',
                            id:restaurant.id,
                            body:restaurant
                        },function(err,response){
                            if(err){
                            	console.log('----------------------------',err)
                                callback(err);
                            }
                            else{
                                console.log(response);
                                callback();
                            }
                        })
                    } else{
                     	//console.log(exists)
                        client.update({
                            index:index_name,
                            type:'my_type',
                            id:restaurant.id,
                            body:{
                                doc:restaurant
                            }
                        },function(error,response){

                            callback();
                        });
                    }
                });
        /*
        }).catch(function(err){
        	console.log('errrrrrrrrrr',err)
            calback(err);
        })
        */
        },function(err){
            if(err)
                console.log('err',err);
            else
                console.log('all done');
        });
    }).catch(function(err){
        console.log(err);
    });
    // mock_data.forEach((value)=>{
    // 	 	client.create({
    //         index:index_name,
    //         type:'my_type',
    //         id:value.id,
    //         body:value
    //     },function(err,response){
    //         if(err){
    //         	console.log('----------------------------',err)
    //             // callback(err);
    //         }
    //         else{
    //             console.log(response);
    //             // callback();
    //         }
    //     })
    // })
}
server.listen(PORT);
function health(req,res,next){
    res.send(200,'search api is okay');
}
function search(req,res,next){
    console.time('search');
    if(!req.query.search_string){
        res.send(200);
        return next();
    }
    const search_string=req.query.search_string;
    const city_search = req.query.city;
    let hits={
        Restaurant:[],
        Category:[],
        Dish:[],
    }
    client.search({
        index:index_name,
        type:'my_type',
        body:{
            query:{
                bool:{
                    should:[
                        {
                            nested:{
                                path:"menu",
                                query:{
                                    multi_match:{
                                        query:search_string,
                                        fields:["menu.name","menu.category"]
                                    }
                                }
                            }
                        },
                        {
                            match:{
                                name:search_string,
                                // city:city_search
                            }
                        },
                        {
                            match:{
                                // name:search_string,
                                city:city_search
                            }
                        }
                    ]
                }
            },
            highlight:{
                fields:{
                    "name":{},
                    "city":{},
                    "menu.category":{},
                    "menu.name":{},

                },
                pre_tags:"",
                post_tags:""
            }
        },
        _source:false
    }).then(function(result){
        docs=result.hits.hits;
        for(i=0;i<docs.length;i++){
            hit=docs[i];
            if (!hit.highlight.city) {continue}
            if(hit.highlight.name){
            	hit.highlight.name.forEach((value)=>{
                        hits.Restaurant.push(value);
                });
            }
            if(hit.highlight['menu.category']){
                hit.highlight['menu.category'].forEach((value)=>{
                    hits['Category'].push(value);
                });
            }
            if(hit.highlight['menu.name']){
                hit.highlight['menu.name'].forEach((value)=>{
                    hits['Dish'].push(value);
                });
            }
            
        }
        console.timeEnd('search');
        res.send(200,hits);
    }).catch(function(error){
        console.log(error);
        res.send(400,error);
    });
}
server.get('/health',health);
server.get('/search',search);
