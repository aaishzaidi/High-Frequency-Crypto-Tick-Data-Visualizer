import React, { useState, useEffect, useRef } from 'react';
import { Activity, Zap, TrendingUp, TrendingDown, AlertTriangle, Power, Server, Cpu, Network, Terminal as TerminalIcon } from 'lucide-react';
import { motion, AnimatePresence } from 'framer-motion';

const BullpenTerminal = () => {
  const [marketData, setMarketData] = useState({});
  const [stats, setStats] = useState({ total: 0, rps: 0 });
  const [status, setStatus] = useState('offline');
  const [isHalted, setIsHalted] = useState(false);
  
  // New UI States for density
  const [liveTape, setLiveTape] = useState([]);
  const [sysLogs, setSysLogs] = useState(["[SYS] Node Engine Initialized", "[KAFKA] Awaiting Broker Connection..."]);
  
  const ws = useRef(null);
  const lastCount = useRef(0);

  useEffect(() => {
    ws.current = new WebSocket('ws://localhost:8080');
    ws.current.onopen = () => {
      setStatus('online');
      addLog("[NET] WebSocket Encrypted Tunnel Established");
    };
    ws.current.onclose = () => {
      setStatus('offline');
      addLog("[NET] Connection Lost. Reconnecting...");
    };
    
    ws.current.onmessage = (event) => {
      const message = JSON.parse(event.data);
      if (message.type === 'UPDATE' && !isHalted) {
        setMarketData(message.data);
        const delta = message.total - lastCount.current;
        lastCount.current = message.total;
        setStats({ total: message.total, rps: delta * 10 });

        // Generate fake "Live Tape" trades based on incoming data
        const symbols = Object.keys(message.data);
        if (symbols.length > 0) {
          const randomSymbol = symbols[Math.floor(Math.random() * symbols.length)];
          const newTrade = {
            id: message.total,
            sym: randomSymbol,
            price: message.data[randomSymbol].p,
            vol: (Math.random() * 5 + 0.1).toFixed(4),
            type: Math.random() > 0.5 ? 'BUY' : 'SELL'
          };
          setLiveTape(prev => [newTrade, ...prev].slice(0, 20)); // Keep last 20
        }

        // Random system logs
        if (Math.random() > 0.95) addLog(`[KAFKA] Batch processed. Offset: ${message.total}`);
      }
    };

    return () => ws.current.close();
  }, [isHalted]);

  const addLog = (msg) => {
    const time = new Date().toISOString().substring(11, 23);
    setSysLogs(prev => [`${time} ${msg}`, ...prev].slice(0, 15));
  };

  const toggleCircuitBreaker = () => {
    const newState = !isHalted;
    setIsHalted(newState);
    addLog(newState ? "[WARN] MANUAL OVERRIDE: INGESTION HALTED" : "[SYS] INGESTION RESUMED");
    if (ws.current && ws.current.readyState === WebSocket.OPEN) {
      ws.current.send(JSON.stringify({ action: newState ? 'HALT' : 'RESUME' }));
    }
  };

  return (
    <div className="min-h-screen bg-black text-emerald-500 font-mono p-2 md:p-4 selection:bg-emerald-900 selection:text-white flex flex-col h-screen overflow-hidden">
      
      {/* HUD Header */}
      <header className="flex justify-between items-center border border-zinc-900 bg-zinc-950/50 p-3 mb-4 shrink-0">
        <div className="flex items-center gap-6">
          <h1 className="text-2xl font-black tracking-tighter text-white flex items-center gap-2">
            <Zap className="text-emerald-500 fill-emerald-500" size={20} />
            BULLPEN<span className="text-zinc-600">.CORE</span>
          </h1>
          <div className="flex gap-2">
            <Badge status={status === 'online'} text={status === 'online' ? 'WS_CONNECTED' : 'OFFLINE'} />
            <Badge status={!isHalted} text={isHalted ? 'INGEST_HALTED' : 'INGEST_ACTIVE'} invert />
          </div>
        </div>

        <div className="flex gap-6 items-center">
          <button 
            onClick={toggleCircuitBreaker}
            className={`flex items-center gap-2 px-4 py-1.5 border text-[10px] font-bold tracking-widest transition-all ${
              isHalted 
                ? 'bg-red-500/20 border-red-500 text-red-500 animate-pulse shadow-[0_0_15px_rgba(239,68,68,0.4)]' 
                : 'bg-zinc-900/50 border-emerald-900 hover:border-red-500 hover:text-red-500 text-emerald-700'
            }`}
          >
            {isHalted ? <AlertTriangle size={12} /> : <Power size={12} />}
            {isHalted ? 'SYSTEM HALTED' : 'CIRCUIT BREAKER'}
          </button>
          <div className="h-8 w-px bg-zinc-800" />
          <StatBox label="THROUGHPUT" value={isHalted ? "0.0" : `${(stats.rps / 1000).toFixed(1)}k`} unit="t/s" />
          <StatBox label="TOTAL INGESTED" value={stats.total.toLocaleString()} unit="TICKS" />
        </div>
      </header>

      {/* Main Dense Grid */}
      <main className="flex-1 grid grid-cols-12 gap-4 min-h-0">
        
        {/* Left Col: Diagnostics & Logs (Span 3) */}
        <div className="col-span-3 flex flex-col gap-4">
          <Panel title="HARDWARE TELEMETRY" icon={<Server size={14} />}>
            <div className="grid grid-cols-2 gap-4 p-2">
              <TelemetryBar label="CPU" val={isHalted ? 4 : Math.random() * 20 + 70} color="bg-emerald-500" />
              <TelemetryBar label="MEM" val={isHalted ? 12 : 64} color="bg-emerald-500" />
              <TelemetryBar label="NET" val={status === 'online' ? 98 : 0} color="bg-blue-500" />
              <TelemetryBar label="IO" val={isHalted ? 0 : Math.random() * 40 + 50} color="bg-purple-500" />
            </div>
          </Panel>
          
          <Panel title="SYSTEM LOGS" icon={<TerminalIcon size={14} />} className="flex-1 overflow-hidden">
            <div className="p-2 space-y-1 overflow-y-auto h-full text-[9px] text-zinc-400 font-mono">
              {sysLogs.map((log, i) => (
                <div key={i} className={`${log.includes('WARN') || log.includes('HALT') ? 'text-red-400' : ''}`}>
                  {log}
                </div>
              ))}
            </div>
          </Panel>
        </div>

        {/* Center Col: Asset Grid (Span 6) */}
        <div className="col-span-6 border border-zinc-900 bg-black overflow-y-auto p-4 custom-scrollbar">
           <div className="flex justify-between items-center mb-4">
              <h2 className="text-xs font-bold tracking-widest text-zinc-500 flex items-center gap-2">
                <Network size={14}/> LIVE ASSET MATRIX
              </h2>
              <span className="text-[9px] text-zinc-600 uppercase">Aggregator Window: 100ms</span>
           </div>
           
           <div className="grid grid-cols-2 gap-3">
            {Object.entries(marketData).map(([symbol, tick]) => (
              <AssetCard key={symbol} symbol={symbol} price={tick.p} timestamp={tick.t} isHalted={isHalted} />
            ))}
            {/* Empty slots for grid styling if less than 4 assets */}
            {Object.keys(marketData).length === 0 && (
                <div className="col-span-2 text-center text-zinc-700 py-20 text-xs tracking-widest animate-pulse">
                  WAITING FOR DATA STREAM...
                </div>
            )}
          </div>
        </div>

        {/* Right Col: Live Order Tape (Span 3) */}
        <div className="col-span-3 flex flex-col">
          <Panel title="LIVE ORDER TAPE" icon={<Activity size={14} />} className="flex-1 overflow-hidden">
            <div className="grid grid-cols-3 text-[9px] border-b border-zinc-900 text-zinc-600 px-2 py-1 tracking-widest">
              <span>SIZE</span>
              <span className="text-center">PRICE</span>
              <span className="text-right">TIME</span>
            </div>
            <div className="overflow-hidden relative h-full">
               <AnimatePresence>
                  {liveTape.map((trade) => (
                    <motion.div 
                      key={trade.id}
                      initial={{ opacity: 0, x: 20 }}
                      animate={{ opacity: 1, x: 0 }}
                      className="grid grid-cols-3 text-[10px] px-2 py-1.5 border-b border-zinc-900/50 hover:bg-zinc-900/30"
                    >
                      <span className={trade.type === 'BUY' ? 'text-emerald-400' : 'text-red-400'}>
                        {trade.vol}
                      </span>
                      <span className="text-white text-center">${trade.price}</span>
                      <span className="text-zinc-500 text-right">{trade.id.toString().slice(-4)}</span>
                    </motion.div>
                  ))}
               </AnimatePresence>
            </div>
          </Panel>
        </div>

      </main>
    </div>
  );
};

// --- Reusable UI Components for Density ---

const Badge = ({ status, text, invert }) => (
  <span className={`text-[9px] px-2 py-0.5 border tracking-widest flex items-center gap-1 ${
    status 
      ? (invert ? 'bg-red-950/30 border-red-900 text-red-500' : 'bg-emerald-950/30 border-emerald-900 text-emerald-500') 
      : (invert ? 'bg-emerald-950/30 border-emerald-900 text-emerald-500' : 'bg-red-950/30 border-red-900 text-red-500')
  }`}>
    <div className={`w-1 h-1 rounded-full ${status ? (invert ? 'bg-red-500' : 'bg-emerald-500') : (invert ? 'bg-emerald-500' : 'bg-red-500')}`} />
    {text}
  </span>
);

const Panel = ({ title, icon, children, className = "" }) => (
  <div className={`border border-zinc-900 bg-zinc-950/30 flex flex-col ${className}`}>
    <div className="text-[10px] tracking-widest text-zinc-500 border-b border-zinc-900 p-2 flex items-center gap-2 bg-black">
      {icon} {title}
    </div>
    {children}
  </div>
);

const TelemetryBar = ({ label, val, color }) => (
  <div className="flex flex-col gap-1">
    <div className="flex justify-between text-[9px] text-zinc-500">
      <span>{label}</span>
      <span>{Math.round(val)}%</span>
    </div>
    <div className="h-1 w-full bg-zinc-900 overflow-hidden">
      <div className={`h-full ${color} transition-all duration-300`} style={{ width: `${val}%` }} />
    </div>
  </div>
);

const StatBox = ({ label, value, unit }) => (
  <div className="text-right flex flex-col justify-center">
    <p className="text-[8px] text-zinc-500 uppercase tracking-widest">{label}</p>
    <p className="text-xl font-bold text-white tracking-tighter tabular-nums leading-none mt-1">
      {value} <span className="text-[10px] text-zinc-600 ml-0.5">{unit}</span>
    </p>
  </div>
);

const AssetCard = ({ symbol, price, timestamp, isHalted }) => {
  const prevPrice = useRef(price);
  const priceHistory = useRef(Array(20).fill(parseFloat(price)));
  const [trend, setTrend] = useState('neutral');

  useEffect(() => {
    if (isHalted) return;
    const numPrice = parseFloat(price);
    if (numPrice > prevPrice.current) setTrend('up');
    else if (numPrice < prevPrice.current) setTrend('down');
    
    prevPrice.current = numPrice;
    priceHistory.current = [...priceHistory.current.slice(1), numPrice];
    
    const timer = setTimeout(() => setTrend('neutral'), 300);
    return () => clearTimeout(timer);
  }, [price, isHalted]);

  const maxP = Math.max(...priceHistory.current);
  const minP = Math.min(...priceHistory.current) || maxP - 1; 
  const range = maxP - minP || 1;
  const points = priceHistory.current.map((p, i) => `${(i / 19) * 100},${100 - ((p - minP) / range) * 100}`).join(' ');

  return (
    <div className={`relative overflow-hidden bg-zinc-950/50 border transition-all duration-300 p-3 group flex flex-col justify-between ${
      isHalted ? 'border-red-900/30 opacity-60' :
      trend === 'up' ? 'border-emerald-500/40 shadow-[inset_0_0_20px_rgba(16,185,129,0.05)]' : 
      trend === 'down' ? 'border-red-500/40 shadow-[inset_0_0_20px_rgba(239,68,68,0.05)]' : 'border-zinc-800'
    }`}>
      <div className="flex justify-between items-start mb-2">
        <span className="text-zinc-300 font-bold tracking-widest text-sm">{symbol}</span>
        <div className={`p-1 border ${trend === 'up' ? 'border-emerald-500/30 text-emerald-400 bg-emerald-500/10' : trend === 'down' ? 'border-red-500/30 text-red-400 bg-red-500/10' : 'border-zinc-800 text-zinc-600'}`}>
          {trend === 'up' ? <TrendingUp size={12}/> : trend === 'down' ? <TrendingDown size={12}/> : <Activity size={12}/>}
        </div>
      </div>
      
      <div className="text-2xl font-mono text-white tracking-tighter tabular-nums mb-2">
        ${price}
      </div>

      <div className="h-10 w-full mb-2 border-b border-zinc-900/50 pb-1">
        <svg viewBox="0 0 100 100" className="w-full h-full preserve-3d" preserveAspectRatio="none">
          <polyline 
            points={points} 
            fill="none" 
            stroke={isHalted ? "#450a0a" : trend === 'down' ? "#ef4444" : "#10b981"} 
            strokeWidth="1.5" 
            vectorEffect="non-scaling-stroke"
          />
        </svg>
      </div>
      
      <div className="flex justify-between items-center">
        <span className="text-[8px] text-zinc-600 font-mono tracking-widest">
          {isHalted ? 'HALTED' : `SYNC: ${new Date(parseInt(timestamp)).getMilliseconds()}MS`}
        </span>
        <span className={`text-[8px] font-bold ${trend === 'up' ? 'text-emerald-500' : trend === 'down' ? 'text-red-500' : 'text-zinc-600'}`}>
           {trend === 'up' ? '+ TICK' : trend === 'down' ? '- TICK' : 'FLAT'}
        </span>
      </div>
    </div>
  );
};

export default BullpenTerminal;