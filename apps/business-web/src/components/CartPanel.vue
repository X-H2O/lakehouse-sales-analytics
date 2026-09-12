<script setup lang="ts">
import { computed, ref } from 'vue'
import { ElMessage } from 'element-plus'
import { createOrder, type CartItem, type CreatedOrder, type User } from '../api/client'

const props = defineProps<{
  user: User
  items: CartItem[]
}>()

const emit = defineEmits<{
  cartChange: [items: CartItem[]]
  orderCreated: [order: CreatedOrder]
}>()

const submitting = ref(false)
const channelId = ref(1)

const totalAmount = computed(() =>
  props.items.reduce((sum, item) => sum + Number(item.list_price) * item.quantity, 0).toFixed(2),
)

function updateQuantity(productId: number, quantity: number) {
  emit(
    'cartChange',
    props.items.map((item) => (item.product_id === productId ? { ...item, quantity } : item)),
  )
}

function removeItem(productId: number) {
  emit(
    'cartChange',
    props.items.filter((item) => item.product_id !== productId),
  )
}

async function submitOrder() {
  if (!props.items.length) {
    ElMessage.warning('订单篮为空')
    return
  }
  submitting.value = true
  try {
    const order = await createOrder(props.user.user_id, channelId.value, props.items)
    emit('orderCreated', order)
    ElMessage.success(`订单已提交：${order.order_id}`)
  } catch (error) {
    ElMessage.error(error instanceof Error ? error.message : '提交失败')
  } finally {
    submitting.value = false
  }
}
</script>

<template>
  <section class="panel cart-panel">
    <div class="panel-title">
      <h2>订单篮</h2>
      <el-tag effect="plain">CNY {{ totalAmount }}</el-tag>
    </div>

    <el-form label-position="top">
      <el-form-item label="渠道 ID">
        <el-input-number v-model="channelId" :min="1" :max="8" class="full-input" />
      </el-form-item>
    </el-form>

    <el-empty v-if="!items.length" description="暂无商品" :image-size="80" />

    <div v-else class="cart-list">
      <div v-for="item in items" :key="item.product_id" class="cart-row">
        <div class="cart-name">
          <strong>{{ item.product_name }}</strong>
          <span>CNY {{ item.list_price }}</span>
        </div>
        <el-input-number :model-value="item.quantity" :min="1" :max="99" size="small" @change="(value: number | undefined) => updateQuantity(item.product_id, value ?? 1)" />
        <el-button size="small" @click="removeItem(item.product_id)">移除</el-button>
      </div>
    </div>

    <el-button type="primary" :loading="submitting" class="submit-button" @click="submitOrder">提交订单</el-button>
  </section>
</template>

<style scoped>
.cart-panel {
  position: sticky;
  top: 90px;
}

.full-input,
.submit-button {
  width: 100%;
}

.cart-list {
  display: grid;
  gap: 12px;
  margin-bottom: 16px;
}

.cart-row {
  display: grid;
  grid-template-columns: minmax(0, 1fr) 112px 64px;
  gap: 8px;
  align-items: center;
  padding: 10px;
  border: 1px solid #e5e7eb;
  border-radius: 8px;
}

.cart-name {
  min-width: 0;
}

.cart-name strong,
.cart-name span {
  display: block;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.cart-name span {
  margin-top: 4px;
  color: #7a8494;
  font-size: 12px;
}
</style>
