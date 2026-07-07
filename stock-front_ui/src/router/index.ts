import { createRouter, createWebHistory } from 'vue-router'
import type { RouteLocationNormalized, RouteRecordRaw } from 'vue-router'
import { jwtDecode, type JwtPayload } from 'jwt-decode'
import { ElMessage } from 'element-plus'

function isTokenExpired(token: string): boolean {
  try {
    const decoded = jwtDecode(token) as JwtPayload & { exp?: number }
    if (!decoded.exp) return false // no expiry claim → trust it
    const now = Date.now() / 1000
    // Add 30s buffer to preemptively redirect before actual expiry
    return decoded.exp < now + 30
  } catch {
    return true // invalid token format → treat as expired
  }
}

const routes: RouteRecordRaw[] = [
  {
    path: '/',
    redirect: '/dashboard',
  },
  {
    path: '/login',
    name: 'Login',
    component: () => import('@/pages/LoginPage.vue'),
  },
  {
    path: '/dashboard',
    name: 'Dashboard',
    component: () => import('@/pages/DashboardPage.vue'),
    meta: { title: '数据平台控制台' },
  },
  {
    path: '/wendgu',
    name: 'Wendgu',
    component: () => import('@/pages/WendguPage.vue'),
    meta: { title: '问股' },
  },
  {
    path: '/selection',
    name: 'Selection',
    component: () => import('@/pages/SelectionPage.vue'),
    meta: { title: '选股工作台' },
  },
  {
    path: '/watchlist',
    name: 'Watchlist',
    component: () => import('@/pages/WatchlistPage.vue'),
    meta: { title: '自选股' },
  },
  {
    path: '/stocks/:symbol',
    name: 'StockDetail',
    component: () => import('@/pages/StockDetailPage.vue'),
    meta: { title: '个股详情' },
  },
  {
    path: '/boards',
    name: 'BoardList',
    component: () => import('@/pages/BoardListPage.vue'),
    meta: { title: '板块分析' },
  },
  {
    path: '/boards/:boardCode',
    name: 'BoardDetail',
    component: () => import('@/pages/BoardDetailPage.vue'),
    meta: { title: '板块详情' },
  },
  {
    path: '/jobs',
    name: 'JobList',
    component: () => import('@/pages/JobListPage.vue'),
    meta: { title: '任务管理' },
  },
  {
    path: '/task-trigger',
    name: 'TaskTrigger',
    component: () => import('@/pages/TaskTriggerPage.vue'),
    meta: { title: '任务触发' },
  },
  {
    path: '/jobs/:jobId',
    name: 'JobDetail',
    component: () => import('@/pages/JobDetailPage.vue'),
    meta: { title: '任务详情' },
  },
  {
    path: '/coverage',
    name: 'Coverage',
    component: () => import('@/pages/CoveragePage.vue'),
    meta: { title: '数据覆盖' },
  },
  {
    path: '/backfill',
    name: 'Backfill',
    component: () => import('@/pages/BackfillPage.vue'),
    meta: { title: '补历史数据' },
  },
  {
    path: '/settings',
    name: 'Settings',
    component: () => import('@/pages/SettingsPage.vue'),
    meta: { title: '系统设置' },
  },
  {
    path: '/:pathMatch(.*)*',
    name: 'NotFound',
    component: () => import('@/pages/NotFoundPage.vue'),
  },
]

const router = createRouter({
  history: createWebHistory(),
  routes,
})

// 路由守卫：登录校验 + Token 过期检测
router.beforeEach(async (to: RouteLocationNormalized, _from: RouteLocationNormalized) => {
  const token = localStorage.getItem('token')

  // 白名单：登录页可直接访问
  if (to.path === '/login') {
    return true
  }

  // 其他页面需要有效登录态
  const expired = token ? isTokenExpired(token) : false
  if (!token || expired) {
    // 清理过期 token 及相关状态
    localStorage.removeItem('token')
    localStorage.removeItem('tokenVerified')
    if (expired) {
      ElMessage?.warning('登录已过期，请重新登录') as any
    }
    return '/login'
  }

  return true
})

export default router
