import { useEffect, useState } from 'react'
import { ShieldAlert, X } from 'lucide-react'

export default function Toast({ message, table, type, onDismiss }) {
  const [exiting, setExiting] = useState(false)

  useEffect(() => {
    const timer = setTimeout(() => {
      setExiting(true)
      setTimeout(onDismiss, 300)
    }, 4000)
    return () => clearTimeout(timer)
  }, [onDismiss])

  return (
    <div className={`fixed top-16 right-4 z-50 ${exiting ? 'opacity-0 translate-x-full' : 'toast-in'} transition-all duration-300`}>
      <div className="flex items-start gap-3 px-4 py-3 rounded-lg min-w-[280px]"
        style={{
          background: 'var(--bg-secondary)',
          border: '1px solid var(--border-primary)',
          borderLeft: '3px solid var(--critical)',
          boxShadow: '0 8px 32px rgba(0,0,0,0.4)',
        }}>
        <ShieldAlert size={16} style={{ color: 'var(--critical)', marginTop: 1 }} />
        <div className="flex-1">
          <div className="text-xs font-semibold uppercase tracking-wide" style={{ color: 'var(--critical)' }}>
            Critical Detected
          </div>
          <div className="font-mono text-xs mt-0.5" style={{ color: 'var(--text-secondary)' }}>
            {table} — {message}
          </div>
        </div>
        <button onClick={onDismiss} className="p-0.5" style={{ color: 'var(--text-muted)' }}>
          <X size={12} />
        </button>
      </div>
    </div>
  )
}
