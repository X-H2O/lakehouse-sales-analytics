<script setup lang="ts">
import { computed, ref } from 'vue'
import { ElMessage } from 'element-plus'
import LoginPage from './pages/LoginPage.vue'
import ProductsPage from './pages/ProductsPage.vue'
import OrdersPage from './pages/OrdersPage.vue'
import type { CartItem, CreatedOrder, User } from './api/client'

const currentUser = ref<User | null>(null)
const activeView = ref<'products' | 'orders'>('products')
const cartItems = ref<CartItem[]>([])
const latestOrder = ref<CreatedOrder | null>(null)

const cartQuantity = computed(() => cartItems.value.reduce((sum, item) => sum + item.quantity, 0))

function handleLogin(user: User) {
  currentUser.value = user
  activeView.value = 'products'
  ElMessage.success(`已登录：${user.user_name}`)
}

function handleAddToCart(item: CartItem) {
  const existing = cartItems.value.find((cartItem) => cartItem.product_id === item.product_id)
  if (existing) {
    existing.quantity += item.quantity
  } else {
    cartItems.value.push(item)
  }
}

function handleCartChange(items: CartItem[]) {
  cartItems.value = items
}

function handleOrderCreated(order: CreatedOrder) {
  latestOrder.value = order
  cartItems.value = []
  activeView.value = 'orders'
}
</script>

<template>
  <div class="app-shell">
    <header class="topbar">
      <div>
        <h1>销售业务模拟系统</h1>
        <span>Business Order Console</span>
      </div>
      <div class="topbar-actions">
        <el-tag v-if="currentUser" effect="plain">{{ currentUser.user_name }} · ID {{ currentUser.user_id }}</el-tag>
        <el-badge :value="cartQuantity" :hidden="cartQuantity === 0">
          <el-button @click="activeView = 'products'">订单篮</el-button>
        </el-badge>
      </div>
    </header>

    <main v-if="!currentUser" class="login-layout">
      <LoginPage @logged-in="handleLogin" />
    </main>

    <main v-else class="work-layout">
      <aside class="sidebar">
        <el-menu :default-active="activeView" @select="(key: string) => (activeView = key as 'products' | 'orders')">
          <el-menu-item index="products">商品下单</el-menu-item>
          <el-menu-item index="orders">最近订单</el-menu-item>
        </el-menu>
      </aside>

      <section class="content">
        <ProductsPage
          v-if="activeView === 'products'"
          :user="currentUser"
          :cart-items="cartItems"
          @add-to-cart="handleAddToCart"
          @cart-change="handleCartChange"
          @order-created="handleOrderCreated"
        />
        <OrdersPage v-else :latest-order="latestOrder" />
      </section>
    </main>
  </div>
</template>
