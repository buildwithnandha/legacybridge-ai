import { FileText, SearchCode, Database, ArrowRight, Shield, Activity, Workflow, Brain } from 'lucide-react'
import ExecutiveSummary from './ExecutiveSummary'
import SchemaDriftTable from './SchemaDriftTable'
import RowReconTable from './RowReconTable'
import CdcAnalysis from './CdcAnalysis'
import RcaFinding from './RcaFinding'

export default function ReportPanel({ report, rootCauses, summary, runId, tablesInvestigated = 0 }) {
  const hasData = report || rootCauses?.length > 0

  return (
    <div className="flex flex-col h-full">
      {/* Panel header */}
      <div className="h-9 flex items-center justify-between px-4"
        style={{ background: 'var(--bg-tertiary)', borderBottom: '1px solid var(--border-primary)' }}>
        <div className="flex items-center gap-2">
          <FileText size={12} style={{ color: 'var(--accent-cyan)' }} />
          <span className="text-[11px] font-semibold uppercase tracking-widest" style={{ color: 'var(--text-secondary)' }}>
            Incident Report
          </span>
        </div>
        {runId && (
          <span className="font-mono text-[11px]" style={{ color: 'var(--text-muted)' }}>
            {runId}
          </span>
        )}
      </div>

      {/* Report content */}
      <div className="flex-1 overflow-y-auto agent-scroll p-4 space-y-5 dot-grid">
        {!hasData ? (
          <ReportEmptyState />
        ) : (
          <>
            <ExecutiveSummary report={report} summary={summary} tablesInvestigated={tablesInvestigated} />
            <SchemaDriftTable report={report} />
            <RowReconTable report={report} />
            <CdcAnalysis report={report} />

            {rootCauses?.length > 0 && (
              <div className="space-y-2">
                <div className="flex items-center gap-2 mb-3">
                  <SearchCode size={13} style={{ color: 'var(--accent-cyan)' }} />
                  <span className="text-[11px] font-semibold uppercase tracking-widest" style={{ color: 'var(--text-secondary)' }}>
                    Root Cause Findings
                  </span>
                </div>
                {rootCauses.map((rc, i) => (
                  <RcaFinding key={i} finding={rc} index={i} />
                ))}
              </div>
            )}
          </>
        )}
      </div>
    </div>
  )
}

function ReportEmptyState() {
  return (
    <div className="h-full flex flex-col items-center justify-center p-6">
      {/* Architecture diagram */}
      <div className="mb-6 w-full max-w-md">
        <div className="text-[10px] uppercase tracking-widest font-medium text-center mb-4" style={{ color: 'var(--text-muted)' }}>
          System Architecture
        </div>

        {/* Pipeline flow */}
        <div className="flex items-center justify-center gap-1 mb-5">
          <FlowNode icon={<Database size={14} />} label="Legacy DB2" sub="Source" />
          <ArrowRight size={14} style={{ color: 'var(--border-accent)' }} />
          <FlowNode icon={<Workflow size={14} />} label="ETL Pipeline" sub="Airflow + Spark" active />
          <ArrowRight size={14} style={{ color: 'var(--border-accent)' }} />
          <FlowNode icon={<Database size={14} />} label="PostgreSQL" sub="Target" />
          <ArrowRight size={14} style={{ color: 'var(--border-accent)' }} />
          <FlowNode icon={<Brain size={14} />} label="AI Agent" sub="RCA Engine" accent />
          <ArrowRight size={14} style={{ color: 'var(--border-accent)' }} />
          <FlowNode icon={<FileText size={14} />} label="Report" sub="PDF + Dashboard" />
        </div>

        {/* Divider */}
        <div className="h-px my-5" style={{ background: 'var(--border-primary)' }} />

        {/* What it detects */}
        <div className="text-[10px] uppercase tracking-widest font-medium text-center mb-3" style={{ color: 'var(--text-muted)' }}>
          Detects &amp; Diagnoses
        </div>
        <div className="grid grid-cols-2 gap-2">
          <DetectCard label="Schema Drift" desc="Missing columns, type mismatches" color="var(--critical)" />
          <DetectCard label="CDC Gaps" desc="Missed change capture events" color="var(--critical)" />
          <DetectCard label="Type Coercion" desc="DECIMAL to FLOAT precision loss" color="var(--warning)" />
          <DetectCard label="TZ Mismatch" desc="UTC offset drift on timestamps" color="var(--warning)" />
          <DetectCard label="Soft Deletes" desc="Excluded rows with status DEL" color="var(--critical)" />
          <DetectCard label="NULL Handling" desc="Empty string vs NULL divergence" color="var(--accent-cyan)" />
        </div>

        {/* Divider */}
        <div className="h-px my-5" style={{ background: 'var(--border-primary)' }} />

        {/* Tech badges */}
        <div className="flex flex-wrap items-center justify-center gap-1.5">
          {['FastAPI', 'PySpark', 'Airflow', 'PostgreSQL', 'React', 'AI Agent', 'SSE Streaming'].map(tech => (
            <span key={tech} className="px-2 py-0.5 rounded text-[9px] font-mono font-medium"
              style={{ background: 'var(--bg-tertiary)', border: '1px solid var(--border-primary)', color: 'var(--text-muted)' }}>
              {tech}
            </span>
          ))}
        </div>
      </div>

      {/* Health score preview */}
      <div className="flex items-center gap-3 px-4 py-2.5 rounded-md"
        style={{ background: 'var(--bg-secondary)', border: '1px solid var(--border-primary)' }}>
        <Activity size={14} style={{ color: 'var(--accent-cyan)' }} />
        <div>
          <div className="text-[11px] font-medium" style={{ color: 'var(--text-secondary)' }}>
            Migration Health Score
          </div>
          <div className="text-[10px]" style={{ color: 'var(--text-muted)' }}>
            Real-time health calculation with severity-weighted scoring
          </div>
        </div>
        <div className="flex items-center gap-1 ml-2">
          <Shield size={12} style={{ color: 'var(--healthy)' }} />
          <span className="font-mono text-xs font-bold" style={{ color: 'var(--text-muted)' }}>--/100</span>
        </div>
      </div>
    </div>
  )
}

function FlowNode({ icon, label, sub, active, accent }) {
  return (
    <div className="flex flex-col items-center gap-1 px-2 py-2 rounded-md min-w-[70px]"
      style={{
        background: accent ? 'rgba(99,102,241,0.08)' : active ? 'var(--bg-tertiary)' : 'var(--bg-secondary)',
        border: `1px solid ${accent ? 'rgba(99,102,241,0.3)' : active ? 'var(--border-accent)' : 'var(--border-primary)'}`,
      }}>
      <span style={{ color: accent ? 'var(--accent-cyan)' : 'var(--text-muted)' }}>{icon}</span>
      <span className="text-[9px] font-semibold" style={{ color: accent ? 'var(--accent-cyan)' : 'var(--text-secondary)' }}>{label}</span>
      <span className="text-[8px]" style={{ color: 'var(--text-muted)' }}>{sub}</span>
    </div>
  )
}

function DetectCard({ label, desc, color }) {
  return (
    <div className="flex items-start gap-2 px-2.5 py-2 rounded-md"
      style={{ background: 'var(--bg-secondary)', border: '1px solid var(--border-primary)' }}>
      <span className="w-1.5 h-1.5 rounded-full mt-1.5 flex-shrink-0" style={{ background: color }} />
      <div>
        <div className="text-[11px] font-medium" style={{ color: 'var(--text-primary)' }}>{label}</div>
        <div className="text-[9px]" style={{ color: 'var(--text-muted)' }}>{desc}</div>
      </div>
    </div>
  )
}
