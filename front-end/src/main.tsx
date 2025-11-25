import React from 'react';
import ReactDOM from 'react-dom/client';
import { ConfigProvider } from 'antd';
import zhCN from 'antd/locale/zh_CN';
import { OpenAPI } from './client';
import App from './App';
import './index.css';

// 1. 指向 Vite 代理的地址
OpenAPI.BASE = '/api/v1'.replace('/api/v1', ''); 

// 2. 自动注入 Token
OpenAPI.TOKEN = async () => {
  return localStorage.getItem('token') || '';
};

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <ConfigProvider locale={zhCN}>
      <App />
    </ConfigProvider>
  </React.StrictMode>,
);