const redis = require('ioredis');
const SimpleMQ = require('./index');

let redis_handler = redis();

const S = new SimpleMQ({
    'task_name': 'test',
    'Q': redis_handler,
    'processor': (msg)=>console.log(msg),
    'prefix': 'smq'
});


for(let i = 0 ; i < 10 ; i++){
	S.unshift({'unshift': i}).then()
}

for(let i = 0 ; i < 10 ; i++){
    S.push({'push': i}).then()
}

S.run(10).then();

setTimeout(()=>{
    console.log('queue empty')
    S.empty().then();
},100);
