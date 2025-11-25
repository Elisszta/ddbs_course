import { Form, Input, Button, Card, Select } from 'antd';
import { useAuth } from '../context/AuthContext';
import { useNavigate } from 'react-router-dom';

export const Login = () => {
  const { login } = useAuth();
  const navigate = useNavigate();

  const onFinish = async (values: any) => {
    await login({ 
      user_id: parseInt(values.user_id), 
      password: values.password 
    });
    // 登录后根据角色简单跳转，实际可更精细
    const role = JSON.parse(localStorage.getItem('user') || '{}').role;
    if (role === 'admin') navigate('/admin/students');
    else if (role === 'student') navigate('/student/courses');
    else navigate('/teacher/courses');
  };

  return (
    <div style={{ height: '100vh', display: 'flex', justifyContent: 'center', alignItems: 'center', background: '#f0f2f5' }}>
      <Card title="分布式抢课系统登录" style={{ width: 400 }}>
        <Form onFinish={onFinish} layout="vertical">
          <Form.Item label="用户ID" name="user_id" rules={[{ required: true }]}>
            <Input placeholder="请输入学号/工号 (如 1120250001)" />
          </Form.Item>
          <Form.Item label="密码" name="password" rules={[{ required: true }]}>
            <Input.Password placeholder="默认密码可能与ID相同" />
          </Form.Item>
          <Button type="primary" htmlType="submit" block>登录</Button>
        </Form>
      </Card>
    </div>
  );
};