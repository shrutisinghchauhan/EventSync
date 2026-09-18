import { createSlice } from '@reduxjs/toolkit';

const socketSlice = createSlice({
  name: 'socket',
  initialState: {
    isOrder1Connected: false,
    isOrder2Connected: false,
    isConsumerConnected: false,
    isPoller_1_top_Connected:false,
    isPoller_2_top_Connected:false,
    isPoller_1_ltu_Connected:false,
    isPoller_2_ltu_Connected:false,
  },
  reducers: {
    setOrder1Connected(state, action) {
      state.isOrder1Connected = action.payload;
    },
    setOrder2Connected(state, action) {
      state.isOrder2Connected = action.payload;
    },
    setConsumerConnected(state, action) {
      state.isConsumerConnected = action.payload;
    },
    setPoller_1_top_Connected(state,action){
      state.isPoller_1_top_Connected=action.payload;
    },
    setPoller_2_top_Connected(state,action){
      state.isPoller_2_top_Connected=action.payload;
    },
    setPoller_1_ltu_Connected(state,action){
      state.isPoller_1_ltu_Connected=action.payload;
    },
    setPoller_2_ltu_Connected(state,action){
      state.isPoller_2_ltu_Connected=action.payload;
    },
  },
});

export const { setOrder1Connected,setOrder2Connected,setConsumerConnected,setPoller_1_top_Connected,setPoller_2_top_Connected,setPoller_1_ltu_Connected,setPoller_2_ltu_Connected}=socketSlice.actions;

export default socketSlice.reducer;