<script setup lang="ts">
import type { CartItem, Product } from '../api/client'

defineProps<{
  products: Product[]
  loading: boolean
}>()

const emit = defineEmits<{ addToCart: [item: CartItem] }>()

function add(product: Product) {
  emit('addToCart', {
    product_id: product.product_id,
    product_name: product.product_name,
    quantity: 1,
    list_price: product.list_price,
  })
}
</script>

<template>
  <el-table :data="products" :loading="loading" height="620" border>
    <el-table-column prop="product_id" label="ID" width="80" />
    <el-table-column prop="product_name" label="商品" min-width="210" show-overflow-tooltip />
    <el-table-column prop="category" label="品类" width="110" />
    <el-table-column prop="brand" label="品牌" width="120" />
    <el-table-column prop="list_price" label="价格" width="110" align="right" />
    <el-table-column label="操作" width="90" fixed="right">
      <template #default="{ row }">
        <el-button type="primary" size="small" @click="add(row)">加入</el-button>
      </template>
    </el-table-column>
  </el-table>
</template>
