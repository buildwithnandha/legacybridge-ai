import { useState, useEffect } from 'react'
import { Zap } from 'lucide-react'

export default function StartupScreen({ onComplete }) {
  const [phase, setPhase] = useState('logo')
  const [progress, setProgress] = useState(0)
  const [statusText, setStatusText] = useState('')
  const [fading, setFading] = useState(false)

  useEffect(() => {
    const startProgress = setTimeout(() => setPhase('progress'), 1500)

    const steps = [
      { at: 1800,  pct: 8,   text: 'Loading core modules...' },
      { at: 2400,  pct: 18,  text: 'Connecting to source database...' },
      { at: 3200,  pct: 30,  text: 'Connecting to target database...' },
      { at: 4000,  pct: 45,  text: 'Indexing CDC event stream...' },
      { at: 5000,  pct: 58,  text: '47,832 change events loaded' },
      { at: 5800,  pct: 72,  text: 'Loading schema registry...' },
      { at: 6600,  pct: 85,  text: 'Initializing AI reconciliation engine...' },
      { at: 7600,  pct: 95,  text: 'Running system diagnostics...' },
      { at: 8400,  pct: 100, text: 'All systems operational' },
    ]

    const timers = steps.map(s =>
      setTimeout(() => {
        setProgress(s.pct)
        setStatusText(s.text)
      }, s.at)
    )

    const statusTimer = setTimeout(() => setPhase('status'), 8400)
    const fadeTimer = setTimeout(() => setFading(true), 9200)
    const completeTimer = setTimeout(() => onComplete(), 9800)

    return () => {
      clearTimeout(startProgress)
      timers.forEach(clearTimeout)
      clearTimeout(statusTimer)
      clearTimeout(fadeTimer)
      clearTimeout(completeTimer)
    }
  }, [onComplete])

  return (
    <div className={`fixed inset-0 z-50 flex flex-col items-center justify-center transition-opacity duration-700 ${fading ? 'opacity-0' : 'opacity-100'}`}
      style={{ background: '#09090B' }}>

      <div className="flex flex-col items-center">
        {/* Icon */}
        <div className="w-20 h-20 rounded-2xl flex items-center justify-center mb-8 transition-all duration-1000"
          style={{
            background: 'rgba(99,102,241,0.08)',
            border: '1px solid rgba(99,102,241,0.2)',
            boxShadow: phase !== 'logo' ? '0 0 60px rgba(99,102,241,0.15)' : 'none',
          }}>
          <Zap size={36} style={{ color: '#818CF8' }} />
        </div>

        {/* Brand name */}
        <div className="flex items-center gap-3 mb-3">
          <span className="text-[32px] font-bold text-white tracking-tight">LegacyBridge</span>
          <span className="text-[32px] font-bold tracking-tight" style={{ color: '#818CF8' }}>AI</span>
        </div>

        {/* Tagline */}
        <div className="text-[14px] tracking-[0.2em] uppercase font-semibold mb-12" style={{ color: '#A1A1AA' }}>
          Data Migration Intelligence
        </div>

        {/* Progress bar */}
        <div className="w-64 h-1 rounded-full overflow-hidden mb-5"
          style={{
            background: 'rgba(255,255,255,0.08)',
            opacity: phase === 'logo' ? 0 : 1,
            transition: 'opacity 0.5s ease',
          }}>
          <div className="h-full rounded-full transition-all ease-out"
            style={{
              width: `${progress}%`,
              background: '#818CF8',
              transitionDuration: '800ms',
              boxShadow: '0 0 12px rgba(99,102,241,0.5)',
            }}
          />
        </div>

        {/* Status text */}
        <div className="h-6 flex items-center justify-center"
          style={{ opacity: phase === 'logo' ? 0 : 1, transition: 'opacity 0.5s ease' }}>
          <span className="text-[13px] font-mono transition-all duration-300" style={{ color: '#71717A' }}>
            {statusText}
          </span>
        </div>
      </div>
    </div>
  )
}
