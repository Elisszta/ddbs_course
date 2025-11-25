import React, { createContext, useContext, useState, useEffect } from 'react';
import { DefaultService, type UserLoginParams, UserLoginResp } from '../client';
import { message } from 'antd';

interface AuthContextType {
  user: UserLoginResp | null;
  login: (data: UserLoginParams) => Promise<void>;
  logout: () => void;
  loading: boolean;
}

const AuthContext = createContext<AuthContextType | null>(null);

export const AuthProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const [user, setUser] = useState<UserLoginResp | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    // 恢复登录状态
    const storedUser = localStorage.getItem('user');
    if (storedUser) {
      setUser(JSON.parse(storedUser));
    }
    setLoading(false);
  }, []);

  const login = async (data: UserLoginParams) => {
    try {
      // 注意：根据你的 JSON，Login 接口没有 tag，可能在 DefaultService 中
      // 如果生成在其他 Service，请相应修改
      const res = await DefaultService.loginApiV1LoginPost(data);
      
      localStorage.setItem('token', res.token);
      localStorage.setItem('user', JSON.stringify(res));
      setUser(res);
      message.success('登录成功');
    } catch (err) {
      message.error('登录失败，请检查账号密码');
      throw err;
    }
  };

  const logout = () => {
    localStorage.removeItem('token');
    localStorage.removeItem('user');
    setUser(null);
    window.location.href = '/login';
  };

  return (
    <AuthContext.Provider value={{ user, login, logout, loading }}>
      {children}
    </AuthContext.Provider>
  );
};

export const useAuth = () => {
  const context = useContext(AuthContext);
  if (!context) throw new Error('useAuth must be used within AuthProvider');
  return context;
};