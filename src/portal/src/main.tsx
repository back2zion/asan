import React from 'react';
import ReactDOM from 'react-dom/client';
import App from './App.tsx'; // Import App.tsx directly with explicit extension
import './index.css';

const root = ReactDOM.createRoot(
  document.getElementById('root') as HTMLElement
);
root.render(
  <React.StrictMode>
    <App /> {/* Render App.tsx directly */}
  </React.StrictMode>
);