<script setup lang="ts">
import { ref } from 'vue'
import { ElMessage } from 'element-plus'
import { login, type User } from '../api/client'

const emit = defineEmits<{ loggedIn: [user: User] }>()
const userId = ref(1)
const loading = ref(false)

async function submit() {
  loading.value = true
  try {
    const result = await login(userId.value)
    emit('loggedIn', result.user)
  } catch (error) {
    ElMessage.error(error instanceof Error ? error.message : '登录失败')
  } finally {
    loading.value = false
  }
}
</script>

<template>
  <section class="login-panel">
    <h2>业务用户登录</h2>
    <el-form label-position="top" @submit.prevent="submit">
      <el-form-item label="用户 ID">
        <el-input-number v-model="userId" :min="1" :controls="false" class="full-input" />
      </el-form-item>
      <el-button type="primary" :loading="loading" class="full-button" @click="submit">登录</el-button>
    </el-form>
  </section>
</template>

<style scoped>
.login-panel {
  width: min(420px, 100%);
  padding: 28px;
  border: 1px solid #e5e7eb;
  border-radius: 8px;
  background: #ffffff;
}

.login-panel h2 {
  margin: 0 0 20px;
  font-size: 20px;
}

.full-input,
.full-button {
  width: 100%;
}
</style>
