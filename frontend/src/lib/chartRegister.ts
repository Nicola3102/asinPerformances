/**
 * Chart.js 全局注册（按需加载的页面入口也应 import 本模块一次）。
 */
import {
  Chart as ChartJS,
  BarController,
  CategoryScale,
  LinearScale,
  LineController,
  BarElement,
  LineElement,
  PointElement,
  Legend,
  Tooltip,
} from 'chart.js'
import { Chart } from 'react-chartjs-2'

ChartJS.register(
  BarController,
  LineController,
  CategoryScale,
  LinearScale,
  BarElement,
  LineElement,
  PointElement,
  Legend,
  Tooltip,
)

export { Chart, ChartJS }
