import { initializeApp } from "https://www.gstatic.com/firebasejs/10.12.0/firebase-app.js";
import { 
  getAuth, 
  createUserWithEmailAndPassword, 
  signInWithEmailAndPassword, 
  signOut,
  GoogleAuthProvider,
  signInWithPopup,
  onAuthStateChanged
} from "https://www.gstatic.com/firebasejs/10.12.0/firebase-auth.js";
import { 
  getFirestore, 
  doc, 
  setDoc, 
  serverTimestamp 
} from "https://www.gstatic.com/firebasejs/10.12.0/firebase-firestore.js";

const firebaseConfig = {
  apiKey: "AIzaSyCstDpqlABfnYeBoDmZLKHV0l7JoUZmcAI",
  authDomain: "diet-analysis-auth.firebaseapp.com",
  projectId: "diet-analysis-auth",
  storageBucket: "diet-analysis-auth.firebasestorage.app",
  messagingSenderId: "1087330333228",
  appId: "1:1087330333228:web:abd931be90582e49035789"
};

const app = initializeApp(firebaseConfig);
const auth = getAuth(app);
const db = getFirestore(app);
const googleProvider = new GoogleAuthProvider();

export async function register(email, password) {
  const userCredential = await createUserWithEmailAndPassword(auth, email, password);
  const user = userCredential.user;
  const token = await user.getIdToken();

  await setDoc(doc(db, "users", user.uid), {
    userId: user.uid,
    email: user.email,
    displayName: user.email.split("@")[0],
    createdAt: serverTimestamp()
  });

  return { user, token };
}

export async function login(email, password) {
  const userCredential = await signInWithEmailAndPassword(auth, email, password);
  const user = userCredential.user;
  const token = await user.getIdToken();
  return { user, token };
}

export async function loginWithGoogle() {
  const result = await signInWithPopup(auth, googleProvider);
  const user = result.user;
  const token = await user.getIdToken();

  
  await setDoc(doc(db, "users", user.uid), {
    userId: user.uid,
    email: user.email,
    displayName: user.displayName,
    createdAt: serverTimestamp()
  }, { merge: true }); 

  return { user, token };
}

export async function logout() {
  await signOut(auth);
}

export function onAuthChange(callback) {
  onAuthStateChanged(auth, callback);
}