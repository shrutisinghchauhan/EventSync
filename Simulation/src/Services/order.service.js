import { toast } from 'react-hot-toast';

const backendURL1 = import.meta.env.VITE_ORDER_SERVICE_1_URL;
const backendURL2 = import.meta.env.VITE_ORDER_SERVICE_2_URL;

export const addOrder = async (formData) => {
  const { batchSize, pattern, quantity , customerName , productName ,failureRate} = formData;
  const patternEndpoints = {
    outbox: '/order/add-order-top',
    listen: '/order/add-order-ltu',
    tailing: '/order/add-order-tlt'
  };

  console.log("BackendUrl ",backendURL1);
  console.log("BackendUrl ",backendURL2);

  const endpointPath = patternEndpoints[pattern];
  toast.loading(`Sending ${batchSize} orders...`, { id: 'batch-send' });
  for (let i = 0; i < batchSize; i++) 
  {
    try 
    {
      const baseUrl = Math.random() < 0.5 ? backendURL1 : backendURL2;
      const fullUrl = `${baseUrl}${endpointPath}`;
      const response = await fetch(fullUrl, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json', 
        },
        body: JSON.stringify({failureRate,orders: [{customerName,productName,quantity,}]}),
      });

      if (!response.ok) {
        console.error(`Order ${i + 1} failed: ${response.statusText}`);
        return 0;
      }

    } catch (error) {
      console.error(`Network error on order ${i + 1}:`, error);
      return 0;
    }
  }
  toast.success('Batch processing complete!', { id: 'batch-send' });
  return 1;
};

export const deleteOrders = async ()=>{
    try 
    {
      const baseUrl = Math.random() < 0.5 ? backendURL1 : backendURL2;
      const endpointPath = "/order/delete-orders";
      const fullUrl = `${baseUrl}${endpointPath}`;
      const response = await fetch(fullUrl, {
        method: 'DELETE'
      });

      if (response.ok) {
        toast.success('Orders Deleted');
      }

    } catch (error) {
        toast.error('Error Deleting Orders');
      console.error('Network error while deleting orders:', error);
    }
};

export const deleteOutbox = async ()=>{
    try 
    {
      const baseUrl = Math.random() < 0.5 ? backendURL1 : backendURL2;
      const endpointPath1 = "/order/delete-outbox2";
      const endpointPath2 = "/order/delete-outbox1";
      const fullUrl1 = `${baseUrl}${endpointPath1}`;
      const fullUrl2 = `${baseUrl}${endpointPath2}`;
      const response1 = await fetch(fullUrl1, {method: 'DELETE'});
      const response2 = await fetch(fullUrl2, {method: 'DELETE'});

      if (response1.ok && response2.ok) {
        toast.success('Outbox Data Deleted');
      }

    } catch (error) {
        toast.error('Error Deleting Orders');
      console.error('Network error while deleting outbox data:', error);
    }
};

export const getOrders = async () => {
  try {
    const baseUrl = Math.random() < 0.5 ? backendURL1 : backendURL2;
    const fullUrl = `${baseUrl}/order/get-orders`;
    const response = await fetch(fullUrl, { method: 'GET' });
    if (!response.ok) throw new Error(`Server error: ${response.status}`);

    const data = await response.json(); 
    toast.success('Orders Fetched');
    return data;
  } catch (error) {
    toast.error('Error Fetching Orders');
    console.error('Network error while fetching orders:', error);
  }
};

export const getOutbox = async () => {
  try 
  {
    const baseUrl = Math.random() < 0.5 ? backendURL1 : backendURL2;
    const fullUrl = `${baseUrl}/order/get-outbox`; 
    const response = await fetch(fullUrl, { method: 'GET' });
    if (!response.ok) throw new Error(`Server error: ${response.status}`);
    const data = await response.json();
    toast.success('Transactional Outbox Data Fetched');
    return data;
  } 
  catch (error) 
  {
    toast.error('Error Fetching Outbox Data');
    console.error('Network error while fetching outbox:', error);
  }
};

export const getListenOutbox = async () => {
  try {
    const baseUrl = Math.random() < 0.5 ? backendURL1 : backendURL2;
    const fullUrl = `${baseUrl}/order/get-outbox-ltu`; 
    const response = await fetch(fullUrl, { method: 'GET' });
    if (!response.ok) throw new Error(`Server error: ${response.status}`);
    const data = await response.json();
    toast.success('Listen-to-Yourself Outbox Data Fetched');
    return data;

  } catch (error) {
    toast.error('Error Fetching Listen-to-Yourself Outbox Data');
    console.error('Network error while fetching LTU outbox:', error);
    throw error;
  }
};
