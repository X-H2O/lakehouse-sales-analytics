<script setup lang="ts">
import { onMounted, ref } from 'vue'
import { ElMessage } from 'element-plus'
import ProductTable from '../components/ProductTable.vue'
import CartPanel from '../components/CartPanel.vue'
import { fetchCategories, fetchProducts, type CartItem, type CreatedOrder, type Product, type User } from '../api/client'

defineProps<{
  user: User
  cartItems: CartItem[]
}>()

const emit = defineEmits<{
  addToCart: [item: CartItem]
  cartChange: [items: CartItem[]]
  orderCreated: [order: CreatedOrder]
}>()

const products = ref<Product[]>([])
const categories = ref<string[]>([])
const category = ref('')
const loading = ref(false)

async function loadProducts() {
  loading.value = true
  try {
    products.value = await fetchProducts(30, category.value || undefined)
  } catch (error) {
    ElMessage.error(error instanceof Error ? error.message : '商品加载失败')
  } finally {
    loading.value = false
  }
}

onMounted(async () => {
  categories.value = await fetchCategories()
  await loadProducts()
})
</script>

<template>
  <div class="grid-two">
    <section class="panel">
      <div class="panel-title">
        <h2>商品下单</h2>
        <el-select v-model="category" clearable placeholder="全部品类" style="width: 180px" @change="loadProducts">
          <el-option v-for="item in categories" :key="item" :label="item" :value="item" />
        </el-select>
      </div>
      <ProductTable :products="products" :loading="loading" @add-to-cart="emit('addToCart', $event)" />
    </section>

    <CartPanel
      :user="user"
      :items="cartItems"
      @cart-change="emit('cartChange', $event)"
      @order-created="emit('orderCreated', $event)"
    />
  </div>
</template>
