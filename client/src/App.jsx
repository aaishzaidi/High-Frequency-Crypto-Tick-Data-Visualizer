import React, { useState, useEffect, useRef } from 'react';
import { Activity, Zap, TrendingUp, TrendingDown } from 'lucide-react';
import { motion } from 'framer-motion';

const BullpenTerminal = () => {
  const [marketData, setMarketData] = useState({});
  const [stats, setStats] = useState({ total: 0, rps: 0 });
  const [status, setStatus] = useState('offline');
  const lastCount = useRef(0);

  useEffect(() => {
    // Connect to your Node.js WebSocket
    const socket = new WebSocket('ws://localhost:8080');

    socket.onopen = () => setStatus('online');
    socket.onclose = () => setStatus('offline');
    
    socket.onmessage = (event) => {
      const message = JSON.parse(event.data);
      if (message.type === 'UPDATE') {
        setMarketData(message.data);
        
        // Calculate Throughput (RPS) based on the 100ms window
        const delta = message.total - lastCount.current;
        lastCount.current = message.total;
        
        setStats({
          total: message.total,
          rps: delta * 10 // Extrapolate 100ms delta to per-second rate
        });
      }
    };

    return () => socket.close();
  }, []);

  return (
    <div className="min-h-screen bg-[#050505] text-emerald-500 font-mono p-6 selection:bg-emerald-900 selection:text-white">
      {/* HUD Header */}
      <div className="flex flex-col md:flex-row justify-between items-start md:items-end border-b border-emerald-900/40 pb-4 mb-8 gap-4">
        <div>
          <h1 className="text-4xl font-black tracking-tighter text-white flex items-center gap-3">
            <Zap className="text-emerald-500 fill-emerald-500" size={32} />
            BULLPEN<span className="text-emerald-800">.SYSTEMS</span>
          </h1>
          <div className="flex gap-4 mt-2 items-center">
            <span className={`text-[10px] px-2 py-0.5 rounded border ${status === 'online' ? 'bg-emerald-950/50 border-emerald-800 text-emerald-400' : 'bg-red-950/50 border-red-800 text-red-400'}`}>
              STREAM: {status === 'online' ? 'ENCRYPTED_ACTIVE' : 'DISCONNECTED'}
            </span>
            <span className="text-[10px] text-emerald-900 uppercase tracking-widest">
              Aggregator Window: 100ms
            </span>
          </div>
        </div>

        <div className="flex gap-8 md:gap-12">
          <StatBox label="THROUGHPUT" value={`${(stats.rps / 1000).toFixed(1)}k`} unit="t/s" />
          <StatBox label="INGESTED" value={stats.total.toLocaleString()} unit="TICKS" />
        </div>
      </div>

      {/* Live Assets Grid */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4 pb-16">
        {Object.entries(marketData).map(([symbol, tick]) => (
          <AssetCard key={symbol} symbol={symbol} price={tick.p} timestamp={tick.t} />
        ))}
      </div>

      {/* System Footer */}
      <div className="fixed bottom-0 left-0 w-full bg-black/90 backdrop-blur-md border-t border-emerald-900/30 p-2 px-8 flex justify-between items-center text-[9px] uppercase text-emerald-900 tracking-tighter z-50">
        <div className="flex gap-4 hidden sm:flex">
          <span>LATENCY_OPTIMIZED: TRUE</span>
          <span>CODEC: PROTOBUF_V3</span>
        </div>
        <div className="flex items-center gap-2">
          <div className={`w-1.5 h-1.5 rounded-full animate-pulse ${status === 'online' ? 'bg-emerald-500' : 'bg-red-500'}`} />
          {status === 'online' ? 'SYSTEM_STABLE' : 'AWAITING_CONNECTION'}
        </div>
      </div>
    </div>
  );
};

const StatBox = ({ label, value, unit }) => (
  <div className="text-right">
    <p className="text-[9px] text-emerald-900 mb-1">{label}</p>
    <p className="text-2xl font-bold text-white tracking-tighter italic">
      {value} <span className="text-xs text-emerald-800 not-italic ml-1">{unit}</span>
    </p>
  </div>
);

const AssetCard = ({ symbol, price, timestamp }) => {
  const prevPrice = useRef(price);
  const [trend, setTrend] = useState('neutral');

  useEffect(() => {
    if (price > prevPrice.current) setTrend('up');
    else if (price < prevPrice.current) setTrend('down');
    
    prevPrice.current = price;
    
    // Clear the visual trend after 300ms to create a "flicker" effect
    const timer = setTimeout(() => setTrend('neutral'), 300);
    return () => clearTimeout(timer);
  }, [price]);

  return (
    <div className={`relative overflow-hidden bg-zinc-900/30 border transition-all duration-300 p-4 rounded-md group ${
      trend === 'up' ? 'border-emerald-500/60 bg-emerald-500/10 shadow-[0_0_15px_rgba(16,185,129,0.2)]' : 
      trend === 'down' ? 'border-red-500/60 bg-red-500/10 shadow-[0_0_15px_rgba(239,68,68,0.2)]' : 'border-emerald-900/30'
    }`}>
      <div className="flex justify-between items-center mb-4">
        <span className="text-emerald-700 font-black tracking-widest text-sm group-hover:text-emerald-400 transition-colors">
          {symbol}
        </span>
        <div className="flex items-center gap-2">
          {trend === 'up' && <TrendingUp size={14} className="text-emerald-400" />}
          {trend === 'down' && <TrendingDown size={14} className="text-red-400" />}
          {trend === 'neutral' && <Activity size={14} className="text-emerald-900" />}
        </div>
      </div>
      
      <div className="text-3xl font-bold text-white tracking-tighter mb-2">
        ${price}
      </div>
      
      <div className="flex justify-between items-end mt-4">
        <div className="w-16 h-[2px] bg-emerald-900/30 rounded-full overflow-hidden">
          <motion.div 
            className={`h-full ${trend === 'down' ? 'bg-red-500' : 'bg-emerald-500'}`}
            animate={{ width: trend !== 'neutral' ? '100%' : '30%' }} 
            transition={{ duration: 0.2 }}
          />
        </div>
        <span className="text-[9px] text-zinc-600 font-sans">
          SYNC: {new Date(parseInt(timestamp)).getMilliseconds()}ms
        </span>
      </div>
    </div>
  );
};

export default BullpenTerminal;