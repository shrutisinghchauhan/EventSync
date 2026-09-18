import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import { Toaster } from 'react-hot-toast'
import './index.css'
import App from './App.jsx'
import store from '../Store/store.js'
import {Provider} from 'react-redux'

createRoot(document.getElementById('root')).render(
  <Provider store={store}>
  <StrictMode>
    <Toaster position='bottom-right' toastOptions={{duration:3000}}/>
    <App />
  </StrictMode>
  </Provider>
)
