import { configureStore } from '@reduxjs/toolkit';
import socketReducer from '../Features/storeslice';

const store = configureStore({
  reducer: {
    socket: socketReducer,
  },
  middleware: (getDefaultMiddleware) =>
    getDefaultMiddleware({
      serializableCheck: false, 
    }),
});

export default store;