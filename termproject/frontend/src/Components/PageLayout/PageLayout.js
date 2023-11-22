import React, { useEffect } from 'react'
import { ConfigProvider, theme, Layout,  } from "antd";
import NavBar from "../NavBar/NavBar";

export default function PageLayout({children, signInRequired = false}) {
  useEffect(() => {
    if (signInRequired) {
      const user = localStorage.getItem('user');
      if (!user) {
        window.location.href = "http://localhost:3000/signin";
      }
    }
  }, [signInRequired]);
  return (
    <Layout theme='light' style={{ minHeight: "100vh", height: "1px" }}>
        <NavBar />
        <Layout.Content>
          {children}
        </Layout.Content>
    </Layout>
  )
}
