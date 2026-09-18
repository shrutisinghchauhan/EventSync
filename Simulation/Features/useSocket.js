import { useEffect } from 'react';
import { useDispatch, useSelector } from 'react-redux';
import { orderSocket1, orderSocket2, consumerSocket, poller_1_top_Socket , poller_2_top_Socket , poller_1_ltu_Socket , poller_2_ltu_Socket } from './socket.js';
import { setOrder1Connected,setOrder2Connected, setConsumerConnected, setPoller_1_ltu_Connected,setPoller_1_top_Connected,setPoller_2_ltu_Connected,setPoller_2_top_Connected } from '../Features/storeslice';

export const useSockets = () => {
  const dispatch = useDispatch();
  
  const statuses = useSelector((state) => state.socket);
  useEffect(() => {
    orderSocket1.connect();
    orderSocket2.connect();
    consumerSocket.connect();
    poller_1_ltu_Socket.connect();
    poller_1_top_Socket.connect();
    poller_2_ltu_Socket.connect();
    poller_2_top_Socket.connect();

    orderSocket1.on('connect', () => {
      console.log('🟢 Order Socket 1 connected! ID:', orderSocket1.id);
      dispatch(setOrder1Connected(true));
    });
    orderSocket1.on('disconnect', () => dispatch(setOrder1Connected(false)));

    orderSocket2.on('connect', () => {
      console.log('🟢 Order Socket 2 connected! ID:', orderSocket2.id);
      dispatch(setOrder2Connected(true));
    });
    orderSocket2.on('disconnect', () => dispatch(setOrder2Connected(false)));

    consumerSocket.on('connect', () => {
      console.log('🟢 Consumer Socket connected! ID:', consumerSocket.id);
      dispatch(setConsumerConnected(true));
    });
    consumerSocket.on('disconnect', () => dispatch(setConsumerConnected(false)));

    poller_1_ltu_Socket.on('connect', () => {
      console.log('🟢 Poller_1_LTU Socket connected! ID:', poller_1_ltu_Socket.id);
      dispatch(setPoller_1_ltu_Connected(true));
    });
    poller_1_ltu_Socket.on('disconnect', () => dispatch(setPoller_1_ltu_Connected(false)));
    
    poller_2_ltu_Socket.on('connect', () => {
      console.log('🟢 Poller_2_LTU Socket connected! ID:', poller_2_ltu_Socket.id);
      dispatch(setPoller_2_ltu_Connected(true));
    });
    poller_2_ltu_Socket.on('disconnect', () => dispatch(setPoller_2_ltu_Connected(false)));

    poller_1_top_Socket.on('connect', () => {
      console.log('🟢 Poller_1_TOP Socket connected! ID:', poller_1_top_Socket.id);
      dispatch(setPoller_1_top_Connected(true));
    });
    poller_1_top_Socket.on('disconnect', () => dispatch(setPoller_1_top_Connected(false)));

    poller_2_top_Socket.on('connect', () => {
      console.log('🟢 Poller_2_TOP Socket connected! ID:', poller_2_top_Socket.id);
      dispatch(setPoller_2_top_Connected(true));
    });
    poller_2_top_Socket.on('disconnect', () => dispatch(setPoller_2_top_Connected(false)));

    return () => {
      orderSocket1.disconnect();
      orderSocket2.disconnect();
      consumerSocket.disconnect();
      poller_1_ltu_Socket.disconnect();
      poller_1_top_Socket.disconnect();
      poller_2_ltu_Socket.disconnect();
      poller_2_top_Socket.disconnect();
      
      orderSocket1.off('connect').off('disconnect');
      orderSocket2.off('connect').off('disconnect');
      consumerSocket.off('connect').off('disconnect');
      poller_1_ltu_Socket.off('connect').off('disconnect');
      poller_1_top_Socket.off('connect').off('disconnect');
      poller_2_ltu_Socket.off('connect').off('disconnect');
      poller_2_top_Socket.off('connect').off('disconnect');
    };
  }, [dispatch]);

  return {orderSocket1,orderSocket2,consumerSocket,statuses,poller_1_ltu_Socket,poller_1_top_Socket,poller_2_ltu_Socket,poller_2_top_Socket};
};