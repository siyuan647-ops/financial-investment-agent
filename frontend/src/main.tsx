import { FormEvent, StrictMode, useEffect, useState } from 'react';
import { createRoot } from 'react-dom/client';
import './main.css';

type ResearchTask = {
  task_id: string;
  status: string;
  phase: string;
  report?: unknown;
  events?: unknown[];
};

async function api<T>(path: string, token: string, init?: RequestInit): Promise<T> {
  const response = await fetch(path, {
    ...init,
    headers: {
      ...(init?.body ? { 'Content-Type': 'application/json' } : {}),
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
      ...init?.headers,
    },
  });
  if (!response.ok) {
    const body = await response.json().catch(() => ({}));
    throw new Error((body as { error?: { message?: string } }).error?.message || `请求失败（${response.status}）`);
  }
  return response.json() as Promise<T>;
}

function App() {
  const [token, setToken] = useState('');
  const [target, setTarget] = useState('601628.SH');
  const [task, setTask] = useState<ResearchTask | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  async function refresh(id: string) {
    try {
      setTask(await api<ResearchTask>(`/api/v1/analysis/research/${id}`, token));
    } catch (reason) {
      setError(String(reason));
    }
  }

  useEffect(() => {
    const id = localStorage.getItem('latest_research_task');
    if (id) void refresh(id);
  }, [token]);

  async function submit(event: FormEvent) {
    event.preventDefault();
    setLoading(true);
    setError('');
    try {
      const accepted = await api<{ task_id: string }>('/api/v1/analysis/query', token, {
        method: 'POST',
        body: JSON.stringify({ target: target.trim() }),
      });
      localStorage.setItem('latest_research_task', accepted.task_id);
      await refresh(accepted.task_id);
    } catch (reason) {
      setError(String(reason));
    } finally {
      setLoading(false);
    }
  }

  async function cancel() {
    if (!task) return;
    setError('');
    try {
      setTask(await api<ResearchTask>(`/api/v1/analysis/research/${task.task_id}/cancel`, token, { method: 'POST' }));
    } catch (reason) {
      setError(String(reason));
    }
  }

  return <main>
    <header><h1>A 股五角色投研</h1><p>DataOps 数据 → 基本面、新闻、市场、投资与风险分析</p></header>
    <section>
      <label>访问令牌 <input type="password" value={token} onChange={event => setToken(event.target.value)} /></label>
      <form onSubmit={submit}>
        <label>证券代码或准确名称 <input value={target} onChange={event => setTarget(event.target.value)} required /></label>
        <button disabled={loading}>{loading ? '提交中…' : '开始研究'}</button>
      </form>
      {error && <p role="alert" className="error">{error}</p>}
    </section>
    {task && <section>
      <div className="status"><h2>研究任务</h2><span>{task.status} · {task.phase}</span></div>
      <p className="task-id">{task.task_id}</p>
      <button onClick={() => void refresh(task.task_id)}>刷新状态</button>
      {['queued', 'running'].includes(task.status) && <button className="secondary" onClick={() => void cancel()}>取消</button>}
      <pre>{JSON.stringify(task.report ?? task.events ?? task, null, 2)}</pre>
    </section>}
  </main>;
}

createRoot(document.getElementById('root')!).render(<StrictMode><App /></StrictMode>);
