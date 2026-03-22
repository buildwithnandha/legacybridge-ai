import { useEffect, useRef, useState } from 'react'
import {
  Sparkles, Database, GitBranch, Radio, Table2, SearchCode, Workflow,
  Clock, MessageSquare, ChevronDown, ChevronRight,
  ShieldCheck, ShieldAlert, AlertTriangle, Loader2, Circle,
} from 'lucide-react'
import ThinkingStep from './ThinkingStep'
import ToolCall from './ToolCall'
import ToolResult from './ToolResult'

const TABLES = [
  { name: 'vendor', desc: 'Vendor master data' },
  { name: 'inventory', desc: 'Inventory items & pricing' },
  { name: 'inventory_transaction', desc: 'Transaction records' },
  { name: 'supplier_contract', desc: 'Supplier agreements' },
  { name: 'purchase_order', desc: 'Purchase orders' },
]

function getTableStatusConfig(status) {
  if (status === 'CRITICAL' || status === 'MISMATCH') return {
    icon: <ShieldAlert size={12} />, color: 'var(--critical)', label: 'Critical',
  }
  if (status === 'WARNING') return {
    icon: <AlertTriangle size={12} />, color: 'var(--warning)', label: 'Warning',
  }
  if (status === 'HEALTHY' || status === 'MATCH') return {
    icon: <ShieldCheck size={12} />, color: 'var(--healthy)', label: 'Healthy',
  }
  if (status === 'SCANNING') return {
    icon: <Loader2 size={12} className="animate-spin" />, color: 'var(--accent-cyan)', label: 'Scanning',
  }
  return {
    icon: <Circle size={10} />, color: 'var(--text-muted)', label: 'Pending',
  }
}

export default function AgentPanel({ steps, isRunning, tableStatuses = {} }) {
  const scrollRef = useRef(null)
  const [elapsed, setElapsed] = useState(0)
  const [treeOpen, setTreeOpen] = useState(true)

  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight
    }
  }, [steps])

  useEffect(() => {
    if (!isRunning) return
    setElapsed(0)
    const interval = setInterval(() => setElapsed(e => e + 1), 1000)
    return () => clearInterval(interval)
  }, [isRunning])

  const hasStatuses = Object.keys(tableStatuses).length > 0

  return (
    <div className="flex flex-col h-full">
      {/* Panel header */}
      <div className="h-10 flex items-center justify-between px-4"
        style={{ background: 'var(--bg-tertiary)', borderBottom: '1px solid var(--border-primary)' }}>
        <div className="flex items-center gap-2.5">
          <div className="w-5 h-5 rounded flex items-center justify-center"
            style={{ background: 'rgba(99,102,241,0.1)' }}>
            <MessageSquare size={11} style={{ color: 'var(--accent-cyan)' }} />
          </div>
          <span className="text-[11px] font-semibold uppercase tracking-widest" style={{ color: 'var(--text-secondary)' }}>
            Agent Activity
          </span>
        </div>
        <div className="flex items-center gap-3">
          {isRunning && (
            <>
              <span className="flex items-center gap-1.5 text-[10px] font-mono" style={{ color: 'var(--text-muted)' }}>
                <Clock size={10} />
                {String(Math.floor(elapsed / 60)).padStart(2, '0')}:{String(elapsed % 60).padStart(2, '0')}
              </span>
              <span className="flex items-center gap-1.5 px-2 py-0.5 rounded-full text-[10px] font-semibold"
                style={{ background: 'rgba(99,102,241,0.08)', border: '1px solid rgba(99,102,241,0.2)', color: 'var(--accent-cyan)' }}>
                <span className="w-1.5 h-1.5 rounded-full pulse-dot" style={{ background: 'var(--accent-cyan)' }} />
                Live
              </span>
            </>
          )}
          {!isRunning && steps.length > 0 && (
            <span className="text-[10px] font-mono px-2 py-0.5 rounded-full"
              style={{ background: 'var(--bg-secondary)', border: '1px solid var(--border-primary)', color: 'var(--text-muted)' }}>
              {steps.length} events
            </span>
          )}
        </div>
      </div>

      {/* Table tree view — only shows when investigation has started */}
      {hasStatuses && (
        <div style={{ background: 'var(--bg-secondary)', borderBottom: '1px solid var(--border-primary)' }}>
          <button
            onClick={() => setTreeOpen(o => !o)}
            className="w-full flex items-center gap-2 px-4 py-2 text-left transition-colors"
            style={{ color: 'var(--text-muted)' }}>
            {treeOpen
              ? <ChevronDown size={12} />
              : <ChevronRight size={12} />
            }
            <Database size={11} />
            <span className="text-[10px] font-semibold uppercase tracking-wider flex-1">
              Tables Under Investigation
            </span>
            <span className="text-[10px] font-mono" style={{ color: 'var(--text-muted)' }}>
              {Object.keys(tableStatuses).length}/5
            </span>
          </button>

          {treeOpen && (
            <div className="px-4 pb-2.5 space-y-0.5">
              {TABLES.map(table => {
                const status = tableStatuses[table.name]
                const config = getTableStatusConfig(status)
                return (
                  <div key={table.name}
                    className="flex items-center gap-2.5 py-1.5 px-2.5 rounded-md transition-all duration-300"
                    style={{
                      background: status ? 'var(--bg-tertiary)' : 'transparent',
                      border: status ? '1px solid var(--border-primary)' : '1px solid transparent',
                    }}>
                    {/* Tree connector line */}
                    <div className="w-3 flex items-center justify-center" style={{ color: 'var(--border-accent)' }}>
                      <span className="text-[10px]">&#9492;</span>
                    </div>

                    {/* Status icon */}
                    <span style={{ color: config.color }}>{config.icon}</span>

                    {/* Table name */}
                    <span className="font-mono text-[11px] flex-1" style={{
                      color: status ? 'var(--text-primary)' : 'var(--text-muted)',
                    }}>
                      {table.name}
                    </span>

                    {/* Status label */}
                    {status && (
                      <span className="text-[9px] font-semibold uppercase tracking-wider px-1.5 py-0.5 rounded"
                        style={{ color: config.color, background: `${config.color}12` }}>
                        {config.label}
                      </span>
                    )}
                  </div>
                )
              })}
            </div>
          )}
        </div>
      )}

      {/* Conversation area */}
      <div ref={scrollRef} className="flex-1 overflow-y-auto agent-scroll"
        style={{ background: 'var(--bg-primary)' }}>
        {steps.length === 0 ? (
          <AgentEmptyState />
        ) : (
          <div className="px-4 py-4 space-y-0">
            {steps.map((step, i) => {
              switch (step.type) {
                case 'start':
                  return (
                    <div key={i} className="flex items-center justify-center mb-4 fade-in">
                      <div className="flex items-center gap-2 px-3 py-1.5 rounded-full"
                        style={{ background: 'var(--bg-secondary)', border: '1px solid var(--border-primary)' }}>
                        <Sparkles size={11} style={{ color: 'var(--accent-cyan)' }} />
                        <span className="text-[11px] font-medium" style={{ color: 'var(--text-secondary)' }}>
                          {step.content}
                        </span>
                      </div>
                    </div>
                  )
                case 'thinking':
                  return <ThinkingStep key={i} content={step.content} />
                case 'tool_call':
                  return <ToolCall key={i} tool={step.tool} input={step.input} />
                case 'tool_result':
                  return <ToolResult key={i} tool={step.tool} result={step.result} />
                default:
                  return null
              }
            })}

            {/* Typing indicator */}
            {isRunning && (
              <div className="flex gap-3 mt-2">
                <div className="w-7 h-7 rounded-full flex items-center justify-center flex-shrink-0"
                  style={{ background: 'rgba(99,102,241,0.1)', border: '1px solid rgba(99,102,241,0.2)' }}>
                  <Sparkles size={13} style={{ color: 'var(--accent-cyan)' }} />
                </div>
                <div className="px-4 py-2.5 rounded-xl rounded-tl-sm"
                  style={{ background: 'var(--bg-secondary)', border: '1px solid var(--border-primary)' }}>
                  <div className="flex gap-1">
                    <span className="w-1.5 h-1.5 rounded-full animate-bounce" style={{ background: 'var(--accent-cyan)', animationDelay: '0ms' }} />
                    <span className="w-1.5 h-1.5 rounded-full animate-bounce" style={{ background: 'var(--accent-cyan)', animationDelay: '150ms' }} />
                    <span className="w-1.5 h-1.5 rounded-full animate-bounce" style={{ background: 'var(--accent-cyan)', animationDelay: '300ms' }} />
                  </div>
                </div>
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  )
}

function AgentEmptyState() {
  const tools = [
    { icon: <GitBranch size={14} />, name: 'Schema Diff', desc: 'Structural drift detection', color: '#3B82F6' },
    { icon: <Table2 size={14} />, name: 'Row Recon', desc: 'Count & checksum validation', color: '#8B5CF6' },
    { icon: <Radio size={14} />, name: 'CDC Analysis', desc: 'Event capture gap detection', color: '#F59E0B' },
    { icon: <Database size={14} />, name: 'Sample Diff', desc: 'Row-level value inspection', color: '#10B981' },
    { icon: <Workflow size={14} />, name: 'Pipeline Logs', desc: 'ETL execution analysis', color: '#6366F1' },
    { icon: <SearchCode size={14} />, name: 'Root Cause', desc: 'AI-powered classification', color: '#818CF8' },
  ]

  return (
    <div className="h-full flex flex-col items-center justify-center p-6">
      <div className="w-12 h-12 rounded-full flex items-center justify-center mb-4"
        style={{ background: 'rgba(99,102,241,0.08)', border: '1px solid rgba(99,102,241,0.2)' }}>
        <Sparkles size={22} style={{ color: 'var(--accent-cyan)' }} />
      </div>

      <span className="text-[14px] font-semibold mb-1" style={{ color: 'var(--text-primary)' }}>
        Reconciliation Agent
      </span>
      <p className="text-[11px] mb-6 text-center max-w-xs leading-relaxed" style={{ color: 'var(--text-muted)' }}>
        I analyze migrated tables using 6 diagnostic tools and stream my reasoning in real time.
      </p>

      <div className="grid grid-cols-3 gap-2 w-full max-w-md mb-5">
        {tools.map(tool => (
          <div key={tool.name} className="flex flex-col items-center gap-1.5 rounded-xl px-3 py-3 text-center transition-all"
            style={{ background: 'var(--bg-secondary)', border: '1px solid var(--border-primary)' }}>
            <div className="w-8 h-8 rounded-lg flex items-center justify-center"
              style={{ background: `${tool.color}10`, border: `1px solid ${tool.color}25` }}>
              <span style={{ color: tool.color }}>{tool.icon}</span>
            </div>
            <div className="text-[10px] font-semibold" style={{ color: 'var(--text-primary)' }}>{tool.name}</div>
            <div className="text-[9px] leading-tight" style={{ color: 'var(--text-muted)' }}>{tool.desc}</div>
          </div>
        ))}
      </div>

      <div className="flex items-center gap-2 px-4 py-2 rounded-full"
        style={{ background: 'var(--bg-secondary)', border: '1px solid var(--border-primary)' }}>
        <span className="text-[11px]" style={{ color: 'var(--text-muted)' }}>Press</span>
        <span className="px-2 py-0.5 rounded text-[10px] font-bold"
          style={{ background: 'rgba(99,102,241,0.1)', color: 'var(--accent-cyan)', border: '1px solid rgba(99,102,241,0.25)' }}>
          Run Analysis
        </span>
        <span className="text-[11px]" style={{ color: 'var(--text-muted)' }}>to start investigation</span>
      </div>
    </div>
  )
}
