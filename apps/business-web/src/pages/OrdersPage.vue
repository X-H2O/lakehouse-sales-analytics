<script setup lang="ts">
import { onMounted, ref } from 'vue'
import { ElMessage } from 'element-plus'
import { fetchRecentOrders, type CreatedOrder, type RecentOrder } from '../api/client'

defineProps<{
  latestOrder: CreatedOrder | null
}>()

const orders = ref<RecentOrder[]>([])
const loading = ref(false)

async function loadOrders() {
  loading.value = true
  try {
    orders.value = await fetchRecentOrders(20)
  } catch (error) {
    ElMessage.error(error instanceof Error ? error.message : '订单加载失败')
  } finally {
    loading.value = false
  }
}

onMounted(loadOrders)
</script>

<template>
  <section class="panel">
    <div class="panel-title">
      <h2>最近订单</h2>
      <el-button @click="loadOrders">刷新</el-button>
    </div>

    <el-alert
      v-if="latestOrder"
      type="success"
      :closable="false"
      show-icon
      class="latest-order"
      :title="`最新订单 ${latestOrder.order_id} 已写入业务库，run_id=${latestOrder.kafka_run_id}`"
    />

    <el-table :data="orders" :loading="loading" height="620" border>
      <el-table-column prop="order_id" label="订单 ID" width="110" />
      <el-table-column prop="user_id" label="用户 ID" width="100" />
      <el-table-column prop="user_name" label="用户" min-width="140" show-overflow-tooltip />
      <el-table-column prop="order_time" label="下单时间" width="190" />
      <el-table-column prop="status" label="状态" width="100" />
      <el-table-column prop="total_amount" label="金额" width="130" align="right" />
    </el-table>
  </section>
</template>

<style scoped>
.latest-order {
  margin-bottom: 14px;
}
</style>
