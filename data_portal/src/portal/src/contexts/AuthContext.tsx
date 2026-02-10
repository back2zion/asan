import React, { createContext, useContext, useState, ReactNode } from 'react';

interface User {
  id: string;
  name: string;
  email: string;
  role: 'patient' | 'doctor' | 'researcher' | 'admin';
  department?: string;
}

interface AuthContextType {
  user: User | null;
  login: (email: string, password: string) => Promise<void>;
  logout: () => void;
  isAuthenticated: boolean;
  isLoading: boolean;
}

const AuthContext = createContext<AuthContextType | undefined>(undefined);

// 데모 계정 매핑
const DEMO_ACCOUNTS: Record<string, User> = {
  'doctor@amc.seoul.kr': { id: 'doctor_001', name: '김의사', email: 'doctor@amc.seoul.kr', role: 'doctor', department: '내과' },
  'researcher@amc.seoul.kr': { id: 'researcher_001', name: '이연구', email: 'researcher@amc.seoul.kr', role: 'researcher', department: '의학연구소' },
  'patient@amc.seoul.kr': { id: 'patient_001', name: '박환자', email: 'patient@amc.seoul.kr', role: 'patient' },
  'admin@amc.seoul.kr': { id: 'admin_001', name: '관리자', email: 'admin@amc.seoul.kr', role: 'admin', department: '의료정보실' },
};

interface AuthProviderProps {
  children: ReactNode;
}

export const AuthProvider: React.FC<AuthProviderProps> = ({ children }) => {
  const [user, setUser] = useState<User | null>(null);
  const [isLoading, setIsLoading] = useState(false);

  const login = async (email: string, _password: string) => {
    setIsLoading(true);
    try {
      await new Promise(resolve => setTimeout(resolve, 800));
      const matched = DEMO_ACCOUNTS[email];
      if (matched) {
        setUser(matched);
      } else {
        // 임의 이메일 → 관리자 (전체 메뉴 접근)
        setUser({
          id: email.split('@')[0],
          name: email.split('@')[0],
          email,
          role: 'admin',
          department: '의료정보실',
        });
      }
    } catch {
      throw new Error('로그인에 실패했습니다.');
    } finally {
      setIsLoading(false);
    }
  };

  const logout = () => {
    setUser(null);
  };

  const value: AuthContextType = {
    user,
    login,
    logout,
    isAuthenticated: !!user,
    isLoading,
  };

  return (
    <AuthContext.Provider value={value}>
      {children}
    </AuthContext.Provider>
  );
};

export const useAuth = (): AuthContextType => {
  const context = useContext(AuthContext);
  if (context === undefined) {
    throw new Error('useAuth must be used within an AuthProvider');
  }
  return context;
};
