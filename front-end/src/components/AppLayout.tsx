import React, { useState } from 'react';
import { Layout, Menu, Button, theme } from 'antd';
import { Outlet, useNavigate, useLocation } from 'react-router-dom';
import { useAuth } from '../context/AuthContext';
import { UserOutlined, BookOutlined, LogoutOutlined, TeamOutlined } from '@ant-design/icons';

const { Header, Sider, Content } = Layout;

export const AppLayout: React.FC = () => {
  const { user, logout } = useAuth();
  const navigate = useNavigate();
  const location = useLocation();
  const {
    token: { colorBgContainer },
  } = theme.useToken();

  const [collapsed, setCollapsed] = useState(false);

  const getMenuItems = () => {
    const role = user?.role;
    const items = [];

    if (role === 'admin') {
      items.push(
        { key: '/admin/students', icon: <UserOutlined />, label: '学生管理' },
        { key: '/admin/teachers', icon: <TeamOutlined />, label: '教师管理' },
        { key: '/admin/courses', icon: <BookOutlined />, label: '课程管理' },
      );
    } else if (role === 'student') {
      items.push(
        { key: '/student/courses', icon: <BookOutlined />, label: '选课中心' },
        { key: '/student/my-courses', icon: <UserOutlined />, label: '我的课表' }
      );
    } else if (role === 'teacher') {
      items.push(
        { key: '/teacher/courses', icon: <BookOutlined />, label: '课程管理' }
      );
    }
    return items;
  };

  return (
    <Layout style={{ minHeight: '100vh' }}>
      <Sider 
        collapsible 
        collapsed={collapsed} 
        onCollapse={(value) => setCollapsed(value)}
        style={{ userSelect: 'none' }}
      >
        <div style={{ 
          height: 32, 
          margin: 16, 
          background: 'rgba(255, 255, 255, 0.2)', 
          textAlign: 'center', 
          color: '#fff', 
          lineHeight: '32px',
          whiteSpace: 'nowrap',
          overflow: 'hidden',
          textOverflow: 'ellipsis',
          transition: 'all 0.2s',
          fontSize: collapsed ? '16px' : '14px',
          fontWeight: collapsed ? 'bold' : 'normal'
        }}>
          {collapsed 
            ? user?.role?.substring(0, 1).toUpperCase() 
            : `${user?.role?.toUpperCase()} 端`
          }
        </div>
        <Menu 
          theme="dark" 
          mode="inline" 
          selectedKeys={[location.pathname]}
          items={getMenuItems()} 
          onClick={(e) => navigate(e.key)}
        />
      </Sider>
      <Layout>
        <Header style={{ padding: '0 24px', background: colorBgContainer, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <span>欢迎, {user?.username}</span>
          <Button type="text" icon={<LogoutOutlined />} onClick={logout}>退出</Button>
        </Header>
        <Content style={{ margin: '16px' }}>
          <div style={{ padding: 24, minHeight: 360, background: colorBgContainer }}>
            <Outlet />
          </div>
        </Content>
      </Layout>
    </Layout>
  );
};