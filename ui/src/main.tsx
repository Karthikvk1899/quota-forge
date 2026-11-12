import {StrictMode, useEffect, useState} from 'react';
import {createRoot} from 'react-dom/client';
import './style.css';

type Decision = {eventId:string; requestId:string; subject:string; allowed:boolean; remaining:number; retryAfterMs:number; timestamp:number; limit:number; windowMs:number};
type Dashboard = {policy:{limit:number;windowMs:number;outboxCapacityPerShard:number}; totals:{allowed?:string;denied?:string};backlog:number[];recent:Decision[]};
function App(){
  const [token,setToken]=useState(''); const [key,setKey]=useState('');
  const [data,setData]=useState<Dashboard>(); const [error,setError]=useState('');
  const [subject,setSubject]=useState('checkout/customer-42');
  const [requestId,setRequestId]=useState(crypto.randomUUID());
  const [decision,setDecision]=useState<Decision>(); const [busy,setBusy]=useState(false);
  const [updated,setUpdated]=useState('');
  useEffect(()=>{
    if(!key) return;
    const abort=new AbortController(); let timer:ReturnType<typeof setTimeout>;
    async function refresh(){
      try{
        const response=await fetch('/api/dashboard',{headers:{Authorization:`Bearer ${key}`},signal:abort.signal});
        if(!response.ok) throw new Error(`Dashboard unavailable (HTTP ${response.status})`);
        setData(await response.json());setError('');setUpdated(new Date().toLocaleTimeString());
      }catch(e){if(!abort.signal.aborted)setError((e as Error).message);}
      finally{if(!abort.signal.aborted)timer=setTimeout(refresh,2000);}
    }
    void refresh(); return ()=>{abort.abort();clearTimeout(timer);};
  },[key]);
  async function send(replay=false){
    setBusy(true);const id=replay?requestId:crypto.randomUUID();setRequestId(id);
    try{
      const res=await fetch('/api/decisions',{method:'POST',headers:{Authorization:`Bearer ${key}`,'Content-Type':'application/json'},body:JSON.stringify({subject,requestId:id})});
      if(res.status!==200&&res.status!==429)throw new Error(`Admission unavailable (HTTP ${res.status}). Retry the same request ID.`);
      setDecision(await res.json());setError('');
    }catch(e){setError((e as Error).message);}finally{setBusy(false);}
  }
  const allowed=Number(data?.totals.allowed??0), denied=Number(data?.totals.denied??0), total=allowed+denied;
  return <main>
    <header><div className="brand"><span className="mark">QF</span><div><b>QUOTA FORGE</b><small>DISTRIBUTED ADMISSION CONTROL</small></div></div><span className="pill">Java 21 / Redis / Kafka</span></header>
    <section className="hero"><div><span className="eyebrow">OPERATIONS CONSOLE</span><h1>Every request.<br/><em>A consistent decision.</em></h1><p>Inspect quota decisions, monitor the audit backlog, and replay requests without spending another permit.</p></div><div className="status"><span className={key&&!error?'dot':'dot muted'}/>{!key?'Connect to begin':error?'Connection degraded':`Polling · ${updated||'connecting'}`}</div></section>
    <form className="connection" onSubmit={e=>{e.preventDefault();setKey(token);setToken('');setData(undefined);}}><label htmlFor="key">Gateway credential</label><input id="key" type="password" autoComplete="off" value={token} onChange={e=>setToken(e.target.value)} placeholder="Paste QUOTA_API_KEY" required minLength={32}/><button>Connect</button>{key&&<button type="button" className="secondary" onClick={()=>{setKey('');setData(undefined);setDecision(undefined);setError('');}}>Disconnect</button>}<small>Kept in memory only. Use a local development credential.</small></form>
    {error&&<p className="error" role="alert">{error}</p>}
    <section className="cards"><article><small>AUDITED DECISIONS</small><h2>{data?total.toLocaleString():'—'}</h2><span>Kafka projection · eventual consistency</span></article><article><small>ADMITTED</small><h2 className="green">{data?allowed.toLocaleString():'—'}</h2><span>{total?`${(100*allowed/total).toFixed(1)}% acceptance`:'Awaiting traffic'}</span></article><article><small>RATE LIMITED</small><h2 className="orange">{data?denied.toLocaleString():'—'}</h2><span>HTTP 429 · retry guidance</span></article><article><small>OUTBOX BACKLOG</small><h2>{data?data.backlog.reduce((a,b)=>a+b,0):'—'}</h2><span>Unacknowledged by Kafka</span></article></section>
    <section className="workbench"><article className="panel"><div className="section-title"><h3>Request laboratory</h3><span className="tag">LIVE API</span></div><p>One permit per unique request. Replays return the original decision for twice the window duration.</p><label htmlFor="subject">Subject / quota bucket</label><input id="subject" value={subject} maxLength={128} pattern="[A-Za-z0-9._:@/\-]+" onChange={e=>{setSubject(e.target.value);setDecision(undefined);setRequestId(crypto.randomUUID());}}/><div className="actions"><button disabled={!key||busy||!subject} onClick={()=>void send()}>Request permit</button><button className="secondary" disabled={!key||busy} onClick={()=>void send(true)}>Replay last ID</button></div><small className="mono">{requestId}</small>{decision&&<div className={'decision '+(decision.allowed?'yes':'no')}><strong>{decision.allowed?'ADMITTED':'RATE LIMITED'}</strong><span>{decision.remaining} remaining · retry after {decision.retryAfterMs} ms</span></div>}</article>
    <article className="panel"><div className="section-title"><h3>Audit outbox / 16 shards</h3><span className="tag">REDIS → KAFKA</span></div><p>{data?`${data.policy.limit} permits / ${data.policy.windowMs/1000}s per subject. Capacity: ${data.policy.outboxCapacityPerShard.toLocaleString()} events per shard.`:'Connect to inspect the active policy.'}</p><div className="shards">{Array.from({length:16},(_,i)=><div key={i}><div className="bar"><i style={{height:data?`${Math.max(2,100*data.backlog[i]/data.policy.outboxCapacityPerShard)}%`:'0%'}}/></div><small>{String(i).padStart(2,'0')}</small><b>{data?.backlog[i]??'—'}</b></div>)}</div><small>New decisions fail closed with HTTP 503 when a shard backlog reaches capacity.</small></article></section>
    <section className="panel"><div className="section-title"><h3>Recent audited decisions</h3><span className="tag">LAST 100 · ARRIVAL ORDER</span></div><div className="table-scroll"><table><thead><tr><th>Time</th><th>Subject hash</th><th>Outcome</th><th>Remaining</th><th>Event ID</th></tr></thead><tbody>{data?.recent.map(d=><tr key={d.eventId}><td>{new Date(d.timestamp).toLocaleTimeString()}</td><td className="mono">{d.subject.slice(0,16)}</td><td><span className={d.allowed?'green':'orange'}>{d.allowed?'ADMITTED':'LIMITED'}</span></td><td>{d.remaining}</td><td className="mono">{d.eventId}</td></tr>)}</tbody></table>{!data?.recent.length&&<div className="empty">No audited decisions yet. Connect and request a permit.</div>}</div></section>
    <footer>Quota Forge · Exact sliding window on the active Redis primary · At-least-once audit delivery</footer>
  </main>;
}
createRoot(document.getElementById('root')!).render(<StrictMode><App/></StrictMode>);
