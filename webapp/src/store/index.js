import Vue from "vue";
import Vuex from "vuex";

Vue.use(Vuex);

let operators = {
    "=": "eq",
    ">": "gt",
    "<": "lt",
    "<=": "le",
    ">=": "ge",
};

export default new Vuex.Store({
    state: {
        socket: {
            isConnected: false,
        },
        topics: ["all"],
        topic: "all",
        messages: [],
        filters: [],
        isRequesting: false,
        size: 20,
        totalSize: 1
    },
    getters: {
        SIZE: (state) => state.size,
        MESSAGES: (state) => state.messages,
        TOPICS: (state) => state.topics,
        TOPIC: (state) => state.topic,
        IS_CONNECTED: (state) => state.socket.isConnected,
        FILTERS: (state) => state.filters,
        IS_REQUESTING: (state) => state.isRequesting,
        TOTAL_PAGE: (state) => parseInt(state.totalSize / state.size)
    },
    mutations: {
        SOCKET_ONOPEN: (state, event) => {
            Vue.prototype.$socket = event.currentTarget;
            state.socket.isConnected = true;
        },
        SOCKET_ONCLOSE: (state) => {
            state.socket.isConnected = false;
        },
        SOCKET_ONERROR: (state, event) => {
            console.error(state, event);
        },
        SOCKET_ONMESSAGE(state, message) {
            message.topic
                ? this.commit("ADD_TOPIC", message.topic.topic)
                : this.commit("ADD_MESSAGE", message.message);
        },
        SET_TOPIC: (state, topic) => {
            state.topic = topic;
        },
        SET_SIZE: (state, size) => {
            state.size = size;
        },
        ADD_TOPIC: (state, topic) => {
            if (!state.topics.includes(topic)) {
                state.topics.push(topic);
            }
        },
        ADD_MESSAGE: (state, message) => {
            state.totalSize = message.offset
            if (state.messages.length === state.size) {
                state.messages.pop();
            }

            state.messages.unshift(message);
        },
        SET_PAGE: (state, page) => {
            state.filters.push({
                operator: ">=",
                parameter: "offset",
                value: (state.totalSize - ((page + 1) * state.size)).toString()
            })
            state.filters.push({
                operator: "<=",
                parameter: "offset",
                value: (state.totalSize - (page * state.size)).toString()
            })
        },
        CLEAR_MESSAGES: (state) => {
            state.messages = [];
        },
        ADD_FILTER: (state, filter) => {
            state.filters.push(filter);
        },
        REMOVE_FILTER: (state, filter) => {
            let index = state.filters.indexOf(filter);

            if (index !== -1) {
                state.filters.splice(index, 1);
            }
        },
        SWITCH_IS_REQUESTING: (state) => {
            state.isRequesting = !state.isRequesting;
        },
    },
    actions: {
        request(ctx) {
            let filters = ctx.getters.FILTERS.map((i) => {
                return {
                    parameter: i.parameter,
                    operator: operators[i.operator],
                    value: i.value,
                };
            });

            filters.push({
                parameter: "topic",
                operator: "eq",
                value: ctx.getters.TOPIC,
            });

            ctx.commit("SWITCH_IS_REQUESTING");
            ctx.commit("CLEAR_MESSAGES");

            Vue.prototype.$socket.sendObj({
                request: "messages",
                filters: filters,
                size: ctx.getters.SIZE
            });

            setTimeout(() => ctx.commit("SWITCH_IS_REQUESTING"), 2000);
        },
    },
    modules: {},
});
