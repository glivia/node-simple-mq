const assert = require("assert");
const sleep = require('thread-sleep');

class SimpleMQ{

    /**
     * 构造函数
     * @param options
     *        options.quene_name : {require} task_name任务名称
     *        options.Q          : {require} redis连接句柄
     *        options.processor  : {require} 处理消息的函数
     *        options.logger     : 打印日志的句柄
     *        options.prefix     : redis中key的前缀
     */
    constructor(options={}) {
        assert(options.task_name, 'options.task_name is required');
        assert(options.Q, 'options.Q is require');
        const PREFIX_SIMPLE_MQ = 'simple_message_queue::';
        const prefix = options.prefix || PREFIX_SIMPLE_MQ;
        this.task_name = options.task_name;
        this.queue_name = `${prefix}${this.task_name}`;
        this.Q = options.Q;
        this.processor = options.processor || this.process;
        this.logger = options.logger || console
    }

    get name(){
        return this.task_name
    }

    _format(msg){
        try{
            return JSON.stringify(msg)
        }catch(e){
            this.logger.error(`[format msg error] \r\nsource: ${msg} \r\nerror: ${e} \r\n\r\n`)
        }
    }

    /**
     * 往队列末尾添加消息
     * @param msg
     * @returns {Promise.<*>}
     */
    async push(msg) {
        try {
            const _msg = this._format(msg);
            await this.Q.rpush(this.queue_name, _msg);
        } catch (err) {
            this.logger.error(`[push msg error] \r\nqueue: ${this.queue_name} \r\nsource: ${_msg} \r\nerror: ${e} \r\n\r\n`)
        }
        return this;
    }

    /**
     * 从队列头部弹出元素
     * @returns {Promise.<*>}
     * @private
     */
    async _pop(){
        try{
            return await this.Q.lpop(this.queue_name);
        }catch(err) {
            this.logger.error(`[pop msg error] \r\nqueue: ${this.queue_name}  \r\nerror: ${e} \r\n\r\n`)
        }
    }

    /**
     * 往队列头部添加消息
     * @param msg
     * @returns {Promise.<*>}
     */
    async unshift(msg){
        try{
            const _msg = this._format(msg);
            await this.Q.lpush(this.queue_name, _msg);
        }catch(err){
            this.logger.error(`[unshift msg error] \r\nqueue: ${this.queue_name} \r\nsource: ${_msg} \r\nerror: ${e} \r\n\r\n`)
        }
        return this;
    }

    static process(msg){
        throw new Error(`process need implement!`)
    }

    async run(delay=0){
        while(1){
            sleep(delay);
            let msg = await this._pop();
            this.processor.call(this, msg);
        }
    }

    async length(){
        return await this.Q.llen(this.queue_name);
    }

    async empty(){
        await this.Q.expire(this.queue_name, 0);
        return this;
    }
}

module.exports = SimpleMQ;