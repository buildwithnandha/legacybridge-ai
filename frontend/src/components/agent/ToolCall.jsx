import { Wrench, GitBranch, Table2, Radio, Database, Workflow, SearchCode, ChevronRight } from 'lucide-react'

const TOOL_META = {
  get_schema_diff:    { icon: GitBranch, label: 'Schema Diff', color: '#3B82F6', desc: 'Comparing source & target schema' },
  get_row_recon:      { icon: Table2, label: 'Row Recon', color: '#8B5CF6', desc: 'Validating row counts & checksums' },
  get_cdc_events:     { icon: Radio, label: 'CDC Analysis', color: '#F59E0B', desc: 'Checking event capture gaps' },
  get_sample_diff:    { icon: Database, label: 'Sample Diff', color: '#10B981', desc: 'Inspecting row-level values' },
  get_pipeline_logs:  { icon: Workflow, label: 'Pipeline Logs', color: '#6366F1', desc: 'Reading ETL execution logs' },
  classify_root_cause:{ icon: SearchCode, label: 'Classify Root Cause', color: '#818CF8', desc: 'Determining root cause' },
}

export default function ToolCall({ tool, input }) {
  const meta = TOOL_META[tool] || { icon: Wrench, label: tool, color: 'var(--accent-cyan)', desc: 'Executing...' }
  const Icon = meta.icon
  const target = Object.values(input || {}).find(v => typeof v === 'string') || ''

  return (
    <div className="flex gap-3 fade-in mb-1 ml-10">
      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-2 px-3 py-2 rounded-lg"
          style={{ background: `${meta.color}08`, border: `1px solid ${meta.color}20` }}>
          {/* Icon */}
          <div className="w-6 h-6 rounded-md flex items-center justify-center flex-shrink-0"
            style={{ background: `${meta.color}15` }}>
            <Icon size={12} style={{ color: meta.color }} />
          </div>

          {/* Info */}
          <div className="flex-1 min-w-0">
            <div className="flex items-center gap-1.5">
              <span className="text-[11px] font-semibold" style={{ color: meta.color }}>
                {meta.label}
              </span>
              {target && (
                <>
                  <ChevronRight size={10} style={{ color: 'var(--text-muted)' }} />
                  <span className="text-[11px] font-mono" style={{ color: 'var(--text-muted)' }}>
                    {target}
                  </span>
                </>
              )}
            </div>
            <div className="text-[10px]" style={{ color: 'var(--text-muted)' }}>
              {meta.desc}
            </div>
          </div>

          {/* Running indicator */}
          <div className="flex items-center gap-1.5 flex-shrink-0">
            <span className="w-1.5 h-1.5 rounded-full pulse-dot" style={{ background: meta.color }} />
          </div>
        </div>
      </div>
    </div>
  )
}
