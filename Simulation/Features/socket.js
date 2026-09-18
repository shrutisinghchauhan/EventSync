import { io } from 'socket.io-client';

const orderServiceURL1 = import.meta.env.VITE_ORDER_SERVICE_1_URL;
const orderServiceURL2 = import.meta.env.VITE_ORDER_SERVICE_2_URL;
const consumerServiceURL = import.meta.env.VITE_CONSUMER_SERVICE_URL;
const poller_1_top=import.meta.env.VITE_POLLER_1_TOP;
const poller_2_top=import.meta.env.VITE_POLLER_2_TOP;
const poller_1_ltu=import.meta.env.VITE_POLLER_1_LTU;
const poller_2_ltu=import.meta.env.VITE_POLLER_2_LTU;

export const orderSocket1 = io(orderServiceURL1, { autoConnect: false });
export const orderSocket2 = io(orderServiceURL2, { autoConnect: false });
export const consumerSocket = io(consumerServiceURL, { autoConnect: false });
export const poller_1_top_Socket = io(poller_1_top,{autoConnect:false});
export const poller_2_top_Socket = io(poller_2_top,{autoConnect:false});
export const poller_1_ltu_Socket = io(poller_1_ltu,{autoConnect:false});
export const poller_2_ltu_Socket = io(poller_2_ltu,{autoConnect:false});