import React, { useEffect } from 'react';
import SignInForm from "../Components/Forms/SignInForm";
import PageLayout from "../Components/PageLayout/PageLayout";

export function useUser() {
    const [user, setUser] = React.useState(null);

    React.useEffect(() => {
        const user = localStorage.getItem('user');
        if (user) {
            setUser(JSON.parse(user));
        }
    }, []);

    return user;
}


export default function SignInPage() {
    const user = useUser();

    useEffect(() => {
        if (user) {
            window.location.href = "http://localhost:3000/workspace";
        }
    }, [user]);
    
    return (
    <PageLayout>
        < SignInForm />
    </PageLayout>
    );

}
