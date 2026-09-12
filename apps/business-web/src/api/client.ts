const API_BASE = import.meta.env.VITE_API_BASE ?? 'http://127.0.0.1:8001'

export interface User {
  user_id: number
  user_name: string
  province: string
  city: string
  channel_id: number
}

export interface Product {
  product_id: number
  sku: string
  product_name: string
  category: string
  brand: string
  list_price: string
}

export interface CartItem {
  product_id: number
  product_name: string
  quantity: number
  list_price: string
}

export interface CreatedOrder {
  order_id: number
  user_id: number
  channel_id: number
  status: string
  total_amount: string
  kafka_run_id: string
}

export interface RecentOrder {
  order_id: number
  user_id: number
  user_name: string
  order_time: string
  status: string
  total_amount: string
}

async function request<T>(path: string, options?: RequestInit): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`, {
    headers: {
      'Content-Type': 'application/json',
      ...(options?.headers ?? {}),
    },
    ...options,
  })

  if (!response.ok) {
    const detail = await response.text()
    throw new Error(detail || `HTTP ${response.status}`)
  }

  return response.json() as Promise<T>
}

export function login(userId: number) {
  return request<{ token: string; user: User }>('/api/login', {
    method: 'POST',
    body: JSON.stringify({ user_id: userId }),
  })
}

export function fetchProducts(limit = 30, category?: string) {
  const params = new URLSearchParams({ limit: String(limit) })
  if (category) params.set('category', category)
  return request<Product[]>(`/api/products?${params}`)
}

export function fetchCategories() {
  return request<string[]>('/api/categories')
}

export function createOrder(userId: number, channelId: number, items: CartItem[]) {
  return request<CreatedOrder>('/api/orders', {
    method: 'POST',
    body: JSON.stringify({
      user_id: userId,
      channel_id: channelId,
      items: items.map((item) => ({
        product_id: item.product_id,
        quantity: item.quantity,
      })),
    }),
  })
}

export function fetchRecentOrders(limit = 20) {
  return request<RecentOrder[]>(`/api/orders/recent?limit=${limit}`)
}
