import { useState, useEffect } from 'react';
import { customFetch } from './fetchUtils';

export const useCurrentUser = () => {
    const [user, setUser] = useState(null);

    useEffect(() => {
        (async () => {
            const response = await customFetch('http://localhost:3300/api/users/');
            setUser(response);
        })();
    }, []);


    return user;
}