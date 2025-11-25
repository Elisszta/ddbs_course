import type { ReactNode } from 'react';
import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import { AuthProvider, useAuth } from './context/AuthContext';
import { AppLayout } from './components/AppLayout';
import { Login } from './pages/Login';
import { StudentManager } from './pages/admin/StudentManager';
import { TeacherManager } from './pages/admin/TeacherManager';
import { CourseManager } from './pages/admin/CourseManager';
import { CourseSelection } from './pages/student/CourseSelection';
import { BatchManager } from './pages/admin/BatchManager';

// 路由保护组件：没登录就踢回 Login 页
const ProtectedRoute = ({ children }: { children: ReactNode }) => {
  const { user, loading } = useAuth();
  if (loading) return <div style={{ padding: 50, textAlign: 'center' }}>Loading...</div>;
  if (!user) return <Navigate to="/login" replace />;
  return children;
};

function App() {
  return (
    // 1. 最外层包裹 AuthProvider，提供登录状态
    <AuthProvider>
      {/* 2. 路由容器 */}
      <BrowserRouter>
        <Routes>
          {/* 公开路由 */}
          <Route path="/login" element={<Login />} />
          
          {/* 受保护的路由 (需要登录) */}
          <Route path="/" element={<ProtectedRoute><AppLayout /></ProtectedRoute>}>
            {/* 默认首页：重定向到对应的功能页，或者显示欢迎 */}
            <Route index element={<div style={{ textAlign: 'center', marginTop: 100, fontSize: 24 }}>欢迎使用分布式抢课系统</div>} />
            
            {/* === 管理员路由 === */}
            <Route path="admin/students" element={<StudentManager />} />
            <Route path="admin/teachers" element={<TeacherManager />} />
            <Route path="admin/courses" element={<CourseManager />} />
            <Route path="admin/selection-batches" element={<BatchManager />} />

            {/* === 学生路由 === */}
            <Route path="student/courses" element={<CourseSelection />} />
            
            {/* 404 路由 */}
            <Route path="*" element={<div style={{ padding: 20 }}>页面不存在</div>} />
          </Route>
        </Routes>
      </BrowserRouter>
    </AuthProvider>
  );
}

export default App;