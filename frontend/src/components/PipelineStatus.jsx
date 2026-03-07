import { useState, useEffect } from 'react'
import { getPipelineStatus } from '../services/api'

const TASK_ORDER = [
  'extract_from_source',
  'transform_data',
  'load_to_target',
  'run_reconciliation',
  'run_rca_agent',
]

const TASK_LABELS = {
  extract_from_source: 'Extract',
  transform_data: 'Transform',
  load_to_target: 'Load',
  run_reconciliation: 'Recon',
  run_rca_agent: 'RCA Agent',
}

const STATUS_STYLES = {
  SUCCESS: 'bg-green-500/20 text-green-400 border-green-500/30',
  FAILED: 'bg-red-500/20 text-red-400 border-red-500/30',
  RUNNING: 'bg-blue-500/20 text-blue-400 border-blue-500/30',
  SKIPPED: 'bg-gray-700/50 text-gray-500 border-gray-600/30',
  PENDING: 'bg-gray-800/50 text-gray-500 border-gray-700/30',
}

export default function PipelineStatus() {
  const [tasks, setTasks] = useState({})

  useEffect(() => {
    getPipelineStatus()
      .then((data) => setTasks(data.tasks || {}))
      .catch(() => {})
  }, [])

  return (
    <div className="bg-gray-900/50 border-b border-gray-800 px-6 py-2">
      <div className="flex items-center gap-2">
        <span className="text-xs text-gray-500 font-medium mr-2">PIPELINE</span>
        {TASK_ORDER.map((taskId, i) => {
          const task = tasks[taskId]
          const status = task?.status || 'PENDING'
          const style = STATUS_STYLES[status] || STATUS_STYLES.PENDING

          return (
            <div key={taskId} className="flex items-center">
              <div
                className={`px-3 py-1 rounded border text-xs font-medium ${style}`}
                title={task ? `${task.duration_seconds}s | ${task.records_processed} records` : ''}
              >
                {TASK_LABELS[taskId]}
                {task?.duration_seconds ? ` (${task.duration_seconds}s)` : ''}
              </div>
              {i < TASK_ORDER.length - 1 && (
                <span className="text-gray-600 mx-1">&#8594;</span>
              )}
            </div>
          )
        })}
      </div>
    </div>
  )
}
