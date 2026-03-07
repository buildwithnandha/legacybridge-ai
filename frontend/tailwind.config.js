/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{js,jsx}'],
  theme: {
    extend: {
      fontFamily: {
        sans: ['Inter', 'system-ui', '-apple-system', 'sans-serif'],
        mono: ['JetBrains Mono', 'Fira Code', 'monospace'],
      },
      colors: {
        brand: {
          50: '#EEF2FF',
          100: '#E0E7FF',
          400: '#818CF8',
          500: '#6366F1',
          600: '#4F46E5',
          700: '#4338CA',
          800: '#3730A3',
          900: '#312E81',
        },
        surface: {
          primary: '#09090B',
          secondary: '#111113',
          tertiary: '#18181B',
          hover: '#27272A',
        },
        border: {
          primary: '#27272A',
          accent: '#3F3F46',
        },
        txt: {
          primary: '#FAFAFA',
          secondary: '#A1A1AA',
          muted: '#71717A',
          code: '#A5B4FC',
        },
        severity: {
          critical: '#EF4444',
          'critical-bg': '#1C1111',
          warning: '#F59E0B',
          'warning-bg': '#1C1709',
          healthy: '#22C55E',
          'healthy-bg': '#0D1A12',
        },
      },
    },
  },
  plugins: [],
}
