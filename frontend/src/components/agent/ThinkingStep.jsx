import { Sparkles } from 'lucide-react'

export default function ThinkingStep({ content }) {
  return (
    <div className="flex gap-3 fade-in mb-3">
      {/* Avatar */}
      <div className="w-7 h-7 rounded-full flex items-center justify-center flex-shrink-0 mt-0.5"
        style={{ background: 'rgba(99,102,241,0.1)', border: '1px solid rgba(99,102,241,0.2)' }}>
        <Sparkles size={13} style={{ color: 'var(--accent-cyan)' }} />
      </div>

      {/* Bubble */}
      <div className="flex-1 min-w-0">
        <div className="text-[10px] font-semibold mb-1" style={{ color: 'var(--accent-cyan)' }}>
          AI Agent
        </div>
        <div className="rounded-xl rounded-tl-sm px-3.5 py-2.5 text-[12px] leading-relaxed"
          style={{
            background: 'var(--bg-secondary)',
            border: '1px solid var(--border-primary)',
            color: 'var(--text-secondary)',
          }}>
          {content}
        </div>
      </div>
    </div>
  )
}
